# frozen_string_literal: true

require "json"

RSpec.describe Sidekiq::Throttled do
  describe ".setup!" do
    it "presets Sidekiq fetch strategy to Sidekiq::Throttled::Fetch" do
      described_class.setup!

      if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
        expect(Sidekiq.default_configuration[:fetch_class]).to eq Sidekiq::Throttled::Fetch7
      else
        expect(Sidekiq[:fetch]).to be_a(Sidekiq::Throttled::Fetch)
      end
    end

    context "with no fetcher configured" do
      it "uses BasicFetch as wrapped fetcher" do
        described_class.setup!

        if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
          expect(Sidekiq.default_configuration[:wrapped_fetcher_class]).to eq Sidekiq::BasicFetch
        else
          expect(Sidekiq[:wrapped_fetcher]).to be_a(Sidekiq::BasicFetch)
        end
      end
    end

    context "with BasicFetch configured" do
      before do
        if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
          Sidekiq.default_configuration[:fetch_class] = Sidekiq::BasicFetch
        else
          Sidekiq[:fetch] = Sidekiq::BasicFetch.new(Sidekiq)
        end
      end

      it "uses BasicFetch as wrapped fetcher" do
        described_class.setup!

        if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
          expect(Sidekiq.default_configuration[:wrapped_fetcher_class]).to eq Sidekiq::BasicFetch
        else
          expect(Sidekiq[:wrapped_fetcher]).to be_a(Sidekiq::BasicFetch)
        end
      end
    end

    if Sidekiq.pro?
      context "with SuperFetch configured" do
        before do
          Sidekiq.super_fetch!
        end

        it "uses SuperFetch as wrapped fetcher" do
          described_class.setup!

          if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
            expect(Sidekiq.default_configuration[:wrapped_fetcher_class]).to eq Sidekiq::BasicFetch
          else
            expect(Sidekiq[:wrapped_fetcher]).to be_a(Sidekiq::Pro::SuperFetch)
          end
        end
      end
    end

    it "injects Sidekiq::Throttled::Middleware server middleware" do
      described_class.setup!

      if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
        expect(Sidekiq.default_configuration.server_middleware.exists?(Sidekiq::Throttled::Middleware))
          .to be true
      else
        expect(Sidekiq.server_middleware.exists?(Sidekiq::Throttled::Middleware))
          .to be true
      end
    end
  end

  describe ".throttled?" do
    it "tolerates invalid JSON message" do
      expect(described_class.throttled?("][")).to be false
    end

    it "tolerates invalid (not fully populated) messages" do
      expect(described_class.throttled?(%({"class" => "foo"}))).to be false
    end

    it "tolerates if limiter not registered" do
      message = %({"class":"foo","jid":#{jid.inspect}})
      expect(described_class.throttled?(message)).to be false
    end

    it "passes JID to registered strategy" do
      strategy = Sidekiq::Throttled::Registry.add("foo",
        threshold:   { limit: 1, period: 1 },
        concurrency: { limit: 1 })

      payload_jid = jid
      message     = %({"class":"foo","jid":#{payload_jid.inspect}})

      expect(strategy).to receive(:throttled?).with payload_jid

      described_class.throttled? message
    end

    it "unwraps ActiveJob-jobs default sidekiq adapter" do
      strategy = Sidekiq::Throttled::Registry.add("wrapped-foo",
        threshold:   { limit: 1, period: 1 },
        concurrency: { limit: 1 })

      payload_jid = jid
      message     = JSON.dump({
        "class"   => "ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper",
        "wrapped" => "wrapped-foo",
        "jid"     => payload_jid
      })

      expect(strategy).to receive(:throttled?).with payload_jid

      described_class.throttled? message
    end

    it "unwraps ActiveJob-jobs custom sidekiq adapter" do
      strategy = Sidekiq::Throttled::Registry.add("JobClassName",
        threshold:   { limit: 1, period: 1 },
        concurrency: { limit: 1 })

      payload_jid = jid
      message     = JSON.dump({
        "class"   => "ActiveJob::QueueAdapters::SidekiqCustomAdapter::JobWrapper",
        "wrapped" => "JobClassName",
        "jid"     => payload_jid
      })

      expect(strategy).to receive(:throttled?).with payload_jid

      described_class.throttled? message
    end
  end

  describe ".recover!" do
    it "tolerates invalid JSON message" do
      expect(described_class.recover!("][")).to be false
    end

    it "tolerates invalid (not fully populated) messages" do
      expect(described_class.recover!(%({"class" => "foo"}))).to be false
    end

    it "tolerates if limiter not registered" do
      message = %({"class":"foo","jid":#{jid.inspect}})
      expect(described_class.recover!(message)).to be false
    end

    it "passes JID to registered strategy" do
      strategy = Sidekiq::Throttled::Registry.add("foo",
        :threshold   => { :limit => 1, :period => 1 },
        :concurrency => { :limit => 1 })

      payload_jid = jid
      message = %({"class":"foo","jid":#{payload_jid.inspect}})

      expect(strategy).to receive(:finalize!).with payload_jid

      described_class.recover! message
    end
  end

  if Sidekiq.pro?
    context "with SuperFetch" do
      before do
        Sidekiq.super_fetch! do
          Kernel.exit
        end

        described_class.setup!
      end

      it "sets up orphan handling" do
        expect(described_class).to receive(:recover!).with("foo")
        expect(Kernel).to receive(:exit)

        Sidekiq[:wrapped_fetcher].notify_orphan("foo")
      end
    end
  end
end
