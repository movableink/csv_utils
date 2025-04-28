require "set"
require_relative "error"
require_relative "csv_utils"

module CsvUtils
  class Validator
    attr_reader :reversed_output_path
    
    def initialize(pattern, error_logs_path: nil, reversed_output_path: nil)
      @pattern = pattern
      @error_logs_path = error_logs_path
      @reversed_output_path = reversed_output_path
      @result = nil
    end

    def validate_rows(file_path)
      @result = CsvUtils._validate(file_path, @pattern, @error_logs_path, @reversed_output_path)
      @result
    end

    def error_summary
      raise CsvUtils::Error, "No result" unless @result
      @result[:errors]
    end

    def error_count
      raise CsvUtils::Error, "No result" unless @result
      @result[:errors].values.sum
    end

    def valid?
      raise CsvUtils::Error, "No result" unless @result
      @result[:errors].empty?
    end

    def row_count
      raise CsvUtils::Error, "No result" unless @result
      @result[:row_count]
    end

    def validate_headers(current:, incoming:)
      current_headers = Set.new(current)
      incoming_headers = Set.new(incoming)

      valid = current_headers.subset?(incoming_headers)

      valid ? [] : generate_header_logs(current:, incoming:)
    end

    def generate_header_logs(current:, incoming:)
      logs = ["Column,Expected Header,Actual Header\n"]
      length = [current.length, incoming.length].max
      length.times do |index|
        logs << "#{index + 1},#{current[index]},#{incoming[index]}\n"
      end
      logs
    end
  end
end