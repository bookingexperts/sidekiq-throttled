# frozen_string_literal: true

# internal
require_relative "registry"

module Sidekiq
  module Throttled
    # Server middleware that notifies strategy that job was finished.
    #
    # @private
    class Middleware
      include Sidekiq::ServerMiddleware

      # Called within Sidekiq job processing
      def call(_worker, msg, _queue)
        yield
      ensure
        Registry.get msg["class"] do |strategy|
          strategy.finalize!(msg["jid"], *msg["args"])
        end
      end
    end
  end
end
