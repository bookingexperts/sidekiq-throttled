# frozen_string_literal: true

source "https://rubygems.org"

gem "appraisal"
gem "rake"

group :test do
  gem "sidekiq"

  gem "capybara"

  gem "rack"
  gem "rack-session"
  gem "rack-test"

  gem "rspec"
  gem "simplecov"
  gem "timecop"

  gem "rubocop",              require: false
  gem "rubocop-capybara",     require: false
  gem "rubocop-performance",  require: false
  gem "rubocop-rake",         require: false
  gem "rubocop-rspec",        require: false
end

group :development, optional: true do
  gem "guard"
  gem "guard-rspec"
end

group :test, :development do
  gem 'debug', '>= 1.0.0'
end

group :doc, optional: true do
  gem "asciidoctor"
  gem "yard"
end

gemspec
