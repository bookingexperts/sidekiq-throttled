# frozen_string_literal: true

require "sidekiq"

require_relative "throttled/version"
require_relative "throttled/configuration"
require_relative "throttled/fetch"
require_relative "throttled/registry"
require_relative "throttled/job"
require_relative "throttled/middleware"
require_relative "throttled/worker"

# @see https://github.com/mperham/sidekiq/
module Sidekiq
  # Concurrency and threshold throttling for Sidekiq.
  #
  # Just add somewhere in your bootstrap:
  #
  #     require "sidekiq/throttled"
  #     Sidekiq::Throttled.setup!
  #
  # Once you've done that you can include {Sidekiq::Throttled::Job} to your
  # job classes and configure throttling:
  #
  #     class MyJob
  #       include Sidekiq::Job
  #       include Sidekiq::Throttled::Job
  #
  #       sidekiq_options :queue => :my_queue
  #
  #       sidekiq_throttle({
  #         # Allow maximum 10 concurrent jobs of this class at a time.
  #         :concurrency => { :limit => 10 },
  #         # Allow maximum 1K jobs being processed within one hour window.
  #         :threshold => { :limit => 1_000, :period => 1.hour }
  #       })
  #
  #       def perform
  #         # ...
  #       end
  #     end
  module Throttled
    class << self
      # @return [Configuration]
      def configuration
        @configuration ||= Configuration.new
      end

      # Hooks throttler into Sidekiq.
      #
      # @return [void]
      def setup!
        Sidekiq.configure_server do |config|
          configure_fetcher!(config)

          config.server_middleware do |chain|
            chain.add(Sidekiq::Throttled::Middleware)
          end
        end
      end

      # Tells whenever job is throttled or not.
      #
      # @param [String] message Job's JSON payload
      # @return [Boolean]
      def throttled?(message)
        with_strategy_and_job(message) do |strategy, jid, args|
          return strategy.throttled?(jid, *args)
        end

        false
      rescue
        false
      end

      # Manually reset throttle for job that had been orphaned.
      #
      # @param [String] message Job's JSON payload
      # @return [Void]
      def recover!(message)
        with_strategy_and_job(message) do |strategy, jid, args|
          strategy.finalize!(jid, *args)
        end
      rescue
        false
      end

      private

      def configure_fetcher!(sidekiq_config)
        if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
          sidekiq_config[:wrapped_fetcher_class] = sidekiq_config.fetch(:fetch_class, Sidekiq::BasicFetch)
          sidekiq_config[:fetch_class] = Sidekiq::Throttled::Fetch7

          if Sidekiq.pro? # SuperFetch
            wrapped_orphan_handler = sidekiq_config[:fetch_setup]

            sidekiq_config[:fetch_setup] = build_orphan_handler(wrapped_orphan_handler)
          end
        else
          wrapped_fetcher =
            sidekiq_config.fetch(:fetch) do
              Sidekiq::BasicFetch.new(sidekiq_config)
            end

          if Sidekiq.pro? && wrapped_fetcher.respond_to?(:orphan_handler) # SuperFetch
            wrapped_orphan_handler = wrapped_fetcher.orphan_handler

            wrapped_fetcher.orphan_handler = build_orphan_handler(wrapped_orphan_handler)
          end

          sidekiq_config[:wrapped_fetcher] = wrapped_fetcher
          sidekiq_config[:fetch] = Sidekiq::Throttled::Fetch.new(sidekiq_config)
        end
      end

      def with_strategy_and_job(message)
        message = JSON.parse(message)

        job = message.fetch("wrapped") { message.fetch("class") { return false } }
        jid = message.fetch("jid") { return false }

        strategy = Registry.get(job)

        yield(strategy, jid, message["args"])
      end

      # Ensure recovered orphaned jobs are unthrottled
      def build_orphan_handler(wrapped_orphan_handler)
        proc do |message, pill|
          recover!(message)

          wrapped_orphan_handler&.call(message, pill)
        end
      end
    end
  end
end
