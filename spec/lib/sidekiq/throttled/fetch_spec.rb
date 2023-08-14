# frozen_string_literal: true

require "sidekiq/api"
require "sidekiq/throttled/fetch"

require "support/working_class_hero"

RSpec.describe Sidekiq::Throttled::Fetch, :sidekiq => :disabled, verify_stubs: false do
  let(:sidekiq_options) do
    { queues: queues }
  end

  let(:queues) { %w[heroes dreamers] }

  subject(:fetcher) { build_tested_class }

  if Sidekiq.pro?
    it "does not patch Sidekiq::BasicFetch with pausable queues support" do
      expect(Sidekiq::BasicFetch).not_to include(Sidekiq::Throttled::Patches::BasicFetch::PausableQueues)
    end
  else
    it "patches Sidekiq::BasicFetch with pausable queues support" do
      expect(Sidekiq::BasicFetch).to include(Sidekiq::Throttled::Patches::BasicFetch::PausableQueues)
    end
  end

  describe ".new" do
    wrapped_fetcher_arg =
      if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
        :wrapped_fetcher_class
      else
        :wrapped_fetcher
      end

    it "fails if #{wrapped_fetcher_arg} is missing" do
      sidekiq_config = build_sidekiq_config(set_wrapped_fetcher: false)

      expect { build_tested_class(sidekiq_config) }.to raise_error(ArgumentError, %r{#{wrapped_fetcher_arg}})
    end

    it "fails if #{wrapped_fetcher_arg} is nil" do
      sidekiq_config = build_sidekiq_config
      sidekiq_config[wrapped_fetcher_arg] = nil

      expect { build_tested_class(sidekiq_config) }.to raise_error(ArgumentError, %r{#{wrapped_fetcher_arg}})
    end

    it "cooldowns queues with TIMEOUT by default" do
      expect(Sidekiq::Throttled::ExpirableList)
        .to receive(:new)
        .with(tested_class::TIMEOUT)
        .and_call_original

      fetcher
    end

    it "allows override throttled queues cooldown period" do
      expect(Sidekiq::Throttled::ExpirableList)
        .to receive(:new)
        .with(1312)
        .and_call_original

      sidekiq_options[:throttled_queue_cooldown] = 1312

      fetcher
    end
  end

  describe "#bulk_requeue" do
    before do
      Sidekiq::Client.push_bulk({
        "class" => WorkingClassHero,
        "args"  => Array.new(3) { [1, 2, 3] }
      })
    end

    it "requeues" do
      works = Array.new(3) { fetcher.retrieve_work }

      queue = Sidekiq::Queue.new("heroes")

      expect(queue.size).to eq(0)

      args =
        if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
          [works]
        else
          [works, {}]
        end

      fetcher.bulk_requeue(*args)

      expect(queue.size).to eq(3)
    end
  end

  describe "#retrieve_work" do
    it "sleeps instead of querying Redis when queues list is empty" do
      fetcher.instance_variable_set(:@throttled_queues, %w[heroes dreamers])

      expect(fetcher.wrapped_fetcher).to receive(:sleep).with(tested_class::TIMEOUT)

      fetcher.wrapped_fetcher.redis do |redis|
        if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
          expect(redis).not_to receive(:blocking_call)
        else
          expect(redis).not_to receive(:brpop)
        end

        expect(fetcher.retrieve_work).to be_nil
      end
    end

    context "when received job is throttled", :time => :frozen do
      before do
        Sidekiq::Client.push_bulk({
          "class" => WorkingClassHero,
          "args"  => Array.new(3) { [] }
        })
      end

      it "pauses job's queue for TIMEOUT seconds" do # rubocop:disable Rspec/MultipleExpectations
        sidekiq_options[:throttled_queue_cooldown] = 0.1

        fetcher.wrapped_fetcher.redis do |redis|
          expect(Sidekiq::Throttled).to receive(:throttled?).and_return(true)
          expect(fetcher.retrieve_work).to be_nil

          if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
            expect(redis)
              .to receive(:blocking_call)
              .with(anything, "brpop", "queue:dreamers", 2)
          else
            expect(redis)
              .to receive(:brpop)
              .with("queue:dreamers", { timeout: 2 })
          end

          # Checks for race condition where the TIMEOUT passed and the queue is re-enabled
          # before it can be reactivated on the original fetcher
          allow(fetcher.wrapped_fetcher).to receive(:retrieve_work).and_wrap_original do |m|
            sleep 0.1

            m.call
          end

          expect(fetcher.retrieve_work).to be_nil

          if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
            expect(redis)
              .to receive(:blocking_call)
              .with(anything, "brpop", "queue:heroes", "queue:dreamers", 2)
          else
            expect(redis)
              .to receive(:brpop)
              .with("queue:heroes", "queue:dreamers", { timeout: 2 })
          end

          expect(fetcher.retrieve_work).to be_nil
        end
      end
    end

    shared_examples "expected behavior" do
      before do
        Sidekiq::Client.push_bulk({
          "class" => WorkingClassHero,
          "args"  => Array.new(10) { [2, 3, 5] }
        })
      end

      subject { fetcher.retrieve_work }

      it { is_expected.not_to be_nil }

      context "when limit is not yet reached" do
        before do
          3.times { fetcher.retrieve_work }
        end

        it { is_expected.not_to be_nil }
      end

      context "when limit exceeded" do
        before do
          5.times { fetcher.retrieve_work }
        end

        it { is_expected.to be_nil }

        it "pushes fetched job back to the queue" do
          fetcher.wrapped_fetcher.redis do |conn|
            expect(conn).to receive(:lpush)

            fetcher.retrieve_work
          end
        end
      end
    end

    context "with static configuration" do
      before do
        WorkingClassHero.sidekiq_throttle(:threshold => {
          :limit  => 5,
          :period => 10
        })
      end

      include_examples "expected behavior"
    end

    context "with dynamic configuration" do
      before do
        WorkingClassHero.sidekiq_throttle(:threshold => {
          :limit  => ->(a, b, _) { a + b },
          :period => ->(a, b, c) { a + b + c }
        })
      end

      include_examples "expected behavior"
    end
  end

  if Sidekiq.pro?
    context "with SuperFetch" do
      before do
        Sidekiq.configure_server do |config|
          config.super_fetch!
        end

        fetcher.wrapped_fetcher.startup

        Sidekiq::Client.push_bulk({
          "class" => WorkingClassHero,
          "args"  => Array.new(3) { [1, 2, 3] }
        })
      end

      let(:queues) { %w[heroes] }

      describe ".new" do
        it "uses SuperFetcher" do
          expect(fetcher.wrapped_fetcher).to be_a(Sidekiq::Pro::SuperFetch)
        end
      end

      describe "#bulk_requeue" do
        it "requeues using rpoplpush/lmove" do
          works = Array.new(3) { fetcher.retrieve_work }
          queue = Sidekiq::Queue.new("heroes")

          expect(queue.size).to eq(0)

          fetcher.wrapped_fetcher.redis do |conn|
            if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.1.0")
              expect(conn)
                .to receive(:lmove).with(%r{queue:sq|.*|heroes}, "queue:heroes", "RIGHT", "LEFT")
                .exactly(4).times
                .and_call_original
            else
              expect(conn)
                .to receive(:rpoplpush).with(%r{queue:sq|.*|heroes}, "queue:heroes")
                .exactly(4).times
                .and_call_original
            end
          end

          fetcher.bulk_requeue(works, sidekiq_options)

          expect(queue.size).to eq(3)
        end
      end

      describe "#retrieve_work" do
        it "retrieves work using brpoplpush" do
          fetcher.wrapped_fetcher.redis do |conn|
            if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.1.0")
              expect(conn)
                .to receive(:blocking_call)
                .with(anything, "BLMOVE", "queue:heroes", %r{queue:sq|.*|heroes}, "RIGHT", "LEFT", 1)
                .and_call_original
            elsif Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
              expect(conn)
                .to receive(:blocking_call)
                .with(anything, "BRPOPLPUSH", "queue:heroes", %r{queue:sq|.*|heroes}, 1)
                .and_call_original
            else
              expect(conn)
                .to receive(:brpoplpush)
                .with("queue:heroes", %r{queue:sq|.*|heroes}, 1)
                .and_call_original
            end
          end

          fetcher.retrieve_work
        end

        it "requeues using the super_requeue script" do
          if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
            Sidekiq::Pro::Scripting.bootstrap(fetcher.wrapped_fetcher.config)
          else
            Sidekiq::Pro::Scripting.bootstrap
          end

          script_sha = Sidekiq::Pro::Scripting::SHAS[:super_requeue]

          fetcher.wrapped_fetcher.redis do |conn|
            allow(Sidekiq::Throttled).to receive(:throttled?).and_return(true)

            if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
              expect(conn)
                .to receive(:call)
                .with("evalsha", script_sha, anything, anything, anything, anything)
                .once
                .and_call_original
            else
              expect(conn)
                .to receive(:evalsha)
                .with(script_sha, anything, anything)
                .once
                .and_call_original
            end
          end

          fetcher.retrieve_work
        end
      end
    end
  end

  private

  def build_tested_class(sidekiq_config = build_sidekiq_config)
    if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
      tested_class.new(sidekiq_config.default_capsule)
    else
      tested_class.new(sidekiq_config)
    end
  end

  def tested_class
    if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
      Sidekiq::Throttled::Fetch7
    else
      Sidekiq::Throttled::Fetch
    end
  end

  def build_sidekiq_config(set_wrapped_fetcher: true)
    if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
      queues = sidekiq_options.delete(:queues)

      Sidekiq::Config.new(sidekiq_options).tap do |sidekiq_config|
        if set_wrapped_fetcher
          sidekiq_config[:wrapped_fetcher_class] =
            Sidekiq.default_configuration.fetch(:fetch_class, Sidekiq::BasicFetch)
        end

        sidekiq_config.queues = queues if queues
      end
    else
      old_fetch = Sidekiq[:fetch]
      Sidekiq.options = Sidekiq::DEFAULTS.merge(sidekiq_options)

      if set_wrapped_fetcher
        Sidekiq[:wrapped_fetcher] = old_fetch || Sidekiq::BasicFetch.new(Sidekiq)
      end

      Sidekiq
    end
  end
end
