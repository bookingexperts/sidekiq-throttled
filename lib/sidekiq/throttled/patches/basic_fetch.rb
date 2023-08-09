# frozen_string_literal: true

# For compatibility with Sidekiq Pro's pausable queues
module Sidekiq::Throttled
  module Patches
    module BasicFetch
      module PausableQueues
        def initialize(options)
          @paused_queues = Set.new

          super
        end

        def notify(action, queue)
          qualified_queue = "queue:#{queue}"

          case action
          when :pause
            @paused_queues << qualified_queue
          when :unpause
            @paused_queues.delete(qualified_queue)
          end
        end

        def queues_cmd
          super - @paused_queues.to_a
        end
      end

      def self.apply!
        Sidekiq::BasicFetch.prepend(PausableQueues) unless Sidekiq::BasicFetch.include?(PausableQueues)
      end
    end
  end
end

Sidekiq::Throttled::Patches::BasicFetch.apply! unless Sidekiq.pro?
