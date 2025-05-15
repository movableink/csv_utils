# frozen_string_literal: true

require "csv_utils"
require "tempfile"
require "set"
require "csv"

RSpec.describe CsvUtils::Validator do
  let(:error_log_path) { Tempfile.new.path }

  describe "Repeated calls to validate_row" do
    it "returns the same status" do
      validator = CsvUtils::Validator.new([
                                            { column_name: "url", validation_type: :url },
                                            { column_name: "url2", validation_type: :url }
                                          ], error_log_path)
      expect(validator.validate_row(["https://example.com", "test.com"])).to eq(false)
      expect(validator.validate_row(["https://example2.com", "test2.com"])).to eq(false)
      expect(validator.status[:total_rows_processed]).to eq(2)
      expect(validator.status[:failed_url_error_count]).to eq(2)
      expect(validator.status[:failed_protocol_error_count]).to eq(0)
    end
  end

  describe "URL validation" do
    it "validates valid URLs" do
      row = [
        "foo",
        "https://example.com",
        "https://test.com/path?query=string#fragment"
      ]

      # Create a pattern with URL validation for the first column
      pattern = [
        { column_name: "name", validation_type: nil },
        { column_name: "url", validation_type: :url },
        { column_name: "url2", validation_type: :url }
      ]

      validator = CsvUtils::Validator.new(pattern, error_log_path)
      expect(validator.validate_row(row)).to eq(true)

      expect(File.read(error_log_path)).to include("Error Message,Row,Column\n")

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(0)
      expect(validator.status[:failed_protocol_error_count]).to eq(0)
    end

    it "catches invalid URLs" do
      data = [
        "url",
        "https://example.com",
        "invalid-url-no-protocol"
      ]

      # Create a pattern with URL validation for the first column
      pattern = [
        { column_name: "url", validation_type: :url }
      ]

      # Expect validation to fail
      validator = CsvUtils::Validator.new(pattern, error_log_path)
      expect(validator.validate_row(data)).to eq(false)

      expect(File.exist?(error_log_path)).to eq(true)
      expect(File.read(error_log_path)).to include("url does not include a valid domain,1,1\n")

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(1)
      expect(validator.status[:failed_protocol_error_count]).to eq(0)
    end
  end

  describe "Protocol validation" do
    it "validates that values have a protocol" do
      data = [
        "value",
        "http://example.com",
        "ftp://example.com",
        "sms://1234567890"
      ]

      # Create a pattern with protocol validation for the first column
      pattern = [
        { column_name: "name", validation_type: nil },
        { column_name: "url", validation_type: :protocol },
        { column_name: "url2", validation_type: :protocol },
        { column_name: "url3", validation_type: :protocol }
      ]

      # Expect no errors
      validator = CsvUtils::Validator.new(pattern, error_log_path)
      expect(validator.validate_row(data)).to eq(true)

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(0)
      expect(validator.status[:failed_protocol_error_count]).to eq(0)
    end

    it "catches values without a protocol" do
      data = [
        "value",
        "example.com",
        "1234567890"
      ]

      # Create a pattern with protocol validation for the first column
      pattern = [
        { column_name: "name", validation_type: nil },
        { column_name: "url", validation_type: :protocol },
        { column_name: "url2", validation_type: nil }
      ]

      # Expect validation to fail
      validator = CsvUtils::Validator.new(pattern, error_log_path)
      expect(validator.validate_row(data)).to eq(false)

      expect(File.exist?(error_log_path)).to eq(true)
      expect(File.read(error_log_path)).to include("url does not include a valid link protocol,1,2\n")

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(0)
      expect(validator.status[:failed_protocol_error_count]).to eq(1)
    end

    it "ignores nil fields" do
      data = [
        "foo",
        ""
      ]

      # Create a pattern with nil validation for the first column
      pattern = [
        { column_name: "name", validation_type: nil },
        { column_name: "url", validation_type: nil }
      ]

      # Expect no errors
      validator = CsvUtils::Validator.new(pattern, error_log_path)
      expect(validator.validate_row(data)).to eq(true)

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(0)
      expect(validator.status[:failed_protocol_error_count]).to eq(0)
    end
  end

  describe "Multi validation" do
    it "validates multiple columns" do
      data = [
        "https://example.com",
        "http://foo.com",
        "John Doe",
        "http://example.com"
      ]

      # Create a pattern with URL, name, and protocol validation
      pattern = [
        { column_name: "url", validation_type: :url },
        { column_name: "url2", validation_type: :url },
        { column_name: "name", validation_type: nil },
        { column_name: "url3", validation_type: :protocol }
      ]

      # Expect no errors
      validator = CsvUtils::Validator.new(pattern, error_log_path)
      validator.validate_row(data)

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(0)
      expect(validator.status[:failed_protocol_error_count]).to eq(0)
    end

    it "returns multiple errors" do
      data = [
        "https://example_com",
        "foo.com",
        "John Doe",
        "example.com"
      ]

      # Create a pattern with URL, name, and protocol validation
      pattern = [
        { column_name: "url", validation_type: :url },
        { column_name: "url2", validation_type: :url },
        { column_name: "name", validation_type: nil },
        { column_name: "url3", validation_type: :protocol }
      ]

      # Expect multiple errors
      validator = CsvUtils::Validator.new(pattern, error_log_path)
      expect(validator.validate_row(data)).to eq(false)

      expect(File.exist?(error_log_path)).to eq(true)
      expect(File.read(error_log_path)).to include("url2 does not include a valid domain,1,2\nurl3 does not include a valid link protocol,1,4\n")

      expect(validator.status[:total_rows_processed]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(1)
      expect(validator.status[:failed_protocol_error_count]).to eq(1)
    end
  end
end
