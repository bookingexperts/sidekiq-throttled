# frozen_string_literal: true

require_relative "expirable_list"
require_relative "patches/basic_fetch"

module Sidekiq
  module Throttled
    # Throttled fetch strategy.
    #
    # @private
    class Fetch
      extend Forwardable

      # Timeout to sleep between fetch retries in case of no job received,
      # as well as timeout to wait for redis to give us something to work.
      TIMEOUT = 2

      # The fetcher that's doing the actual work
      attr_reader :wrapped_fetcher

      # Initializes fetcher instance.
      # @param sidekiq_config [Hash]
      # @option sidekiq_config [Integer] :throttled_queue_cooldown (TIMEOUT)
      #   Min delay in seconds before queue will be polled again after
      #   throttled job.
      def initialize(sidekiq_config)
        @wrapped_fetcher = sidekiq_config[:wrapped_fetcher]

        raise ArgumentError, ":wrapped_fetcher not set" if @wrapped_fetcher.nil?

        initialize_throttled_queues(sidekiq_config)
      end

      # Retrieves job from wrapped fetcher.
      #
      # @return [Sidekiq::*::UnitOfWork, nil]
      def retrieve_work
        work =
          without_queues(@throttled_queues.to_a) do
            wrapped_fetcher.retrieve_work
          end

        if work && Throttled.throttled?(work.job)
          requeue_throttled(work) # Requeue in back of queue

          @throttled_queues << work.queue_name # Ensure queue is not polled again directly

          nil
        else
          work
        end
      end

      def_delegators :wrapped_fetcher, :bulk_requeue

      private

      def initialize_throttled_queues(sidekiq_config)
        @throttled_queues = ExpirableList.new(sidekiq_config.fetch(:throttled_queue_cooldown, TIMEOUT))
      end

      # Executes block without the passed queues active on the original fetcher
      # Compatible with BasicFetch & SuperFetch
      #
      # @param [Array<String>] queues
      #   The queues that should be inactive for this block
      def without_queues(queues, &_block)
        queues.each do |queue|
          wrapped_fetcher.notify(:pause, queue)
        end

        yield
      ensure
        queues.each do |queue|
          wrapped_fetcher.notify(:unpause, queue)
        end
      end

      def requeue_throttled(work)
        if work.respond_to?(:local_queue) # SuperFetch
          work.requeue
        else
          wrapped_fetcher.redis do |conn|
            conn.lpush(work.queue, work.job)
          end
        end
      end
    end

    # Use for Sidekiq 7+
    class Fetch7 < Fetch
      def initialize(capsule) # rubocop:disable Lint/MissingSuper
        sidekiq_config = capsule.config

        wrapped_fetcher_class = sidekiq_config[:wrapped_fetcher_class]

        raise ArgumentError, ":wrapped_fetcher_class not set" if wrapped_fetcher_class.nil?

        @wrapped_fetcher = wrapped_fetcher_class.new(capsule)

        initialize_throttled_queues(sidekiq_config)
      end

      # For SuperFetch
      if Sidekiq.pro?
        def_delegators :wrapped_fetcher, :register_myself, :setup, :terminate
      end
    end
  end
end
