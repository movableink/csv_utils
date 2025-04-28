# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

require "rubocop/rake_task"

RuboCop::RakeTask.new

require "rb_sys/extensiontask"

task build: :compile

GEMSPEC = Gem::Specification.load("csv_utils.gemspec")

RbSys::ExtensionTask.new("csv_utils", GEMSPEC) do |ext|
  ext.lib_dir = "lib/csv_utils"
end

task default: %i[compile spec rubocop]
