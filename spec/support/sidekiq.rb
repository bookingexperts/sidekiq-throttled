# frozen_string_literal: true

require "logger"
require "securerandom"
require "singleton"
require "stringio"

require "sidekiq"
require "sidekiq/cli"

begin
  require "sidekiq-pro"
rescue LoadError
  true
end

$TESTING = true # rubocop:disable Style/GlobalVars

REDIS_URL = ENV.fetch("REDIS_URL", "redis://localhost:6379")

module JidGenerator
  def jid
    SecureRandom.hex 12
  end
end

class PseudoLogger < Logger
  include Singleton

  def initialize
    @io = StringIO.new
    super(@io)
  end

  def reset!
    @io.reopen
  end

  def output
    @io.string
  end
end

if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
  Sidekiq.configure_server do |config|
    config.queues = %i[default]
  end
else
  Sidekiq[:queues] = %i[default]
end

Sidekiq.configure_server do |config|
  config.redis  = { url: REDIS_URL }
  config.logger = PseudoLogger.instance
end

Sidekiq.configure_client do |config|
  config.redis  = { url: REDIS_URL }
  config.logger = PseudoLogger.instance
end

RSpec.configure do |config|
  config.include JidGenerator
  config.extend  JidGenerator

  config.before do
    PseudoLogger.instance.reset!

    Sidekiq.redis do |conn|
      conn.flushdb
      conn.script("flush")
    end
  end

  config.around do |ex|
    @_old_sidekiq_options =
      if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
        Sidekiq.default_configuration.instance_variable_get(:@options).dup
      else
        Sidekiq.options.dup
      end

    ex.run

    if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("7.0.0")
      Sidekiq.default_configuration.instance_variable_set(:@options, @_old_sidekiq_options)
    else
      Sidekiq.options = @_old_sidekiq_options
    end
  end
end
