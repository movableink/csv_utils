# frozen_string_literal: true

require "csv_utils"
require "tempfile"
require "set"
require "csv"

RSpec.describe CsvUtils::Validator do
  describe "Repeated calls to validate_row" do
    it "returns the same status" do
      validator = CsvUtils::Validator.new([:url, :url])
      expect(validator.validate_row(["https://example.com", "test.com"])).to eq(false)
      expect(validator.validate_row(["https://example2.com", "test2.com"])).to eq(false)
      expect(validator.status[:total_rows]).to eq(2)
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
      pattern = [nil, :url, :url]
      
      validator = CsvUtils::Validator.new(pattern)
      expect(validator.validate_row(row)).to eq(true)

      expect(validator.status[:total_rows]).to eq(1)
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
      pattern = [:url]
      
      # Expect validation to fail
      validator = CsvUtils::Validator.new(pattern)
      expect(validator.validate_row(data)).to eq(false)

      expect(validator.status[:total_rows]).to eq(1)
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
      pattern = [nil, :protocol, :protocol, :protocol]
      
      # Expect no errors
      validator = CsvUtils::Validator.new(pattern)
      expect(validator.validate_row(data)).to eq(true)

      expect(validator.status[:total_rows]).to eq(1)
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
      pattern = [nil, :protocol, nil]
      
      # Expect validation to fail
      validator = CsvUtils::Validator.new(pattern)
      expect(validator.validate_row(data)).to eq(false)

      expect(validator.status[:total_rows]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(0)
      expect(validator.status[:failed_protocol_error_count]).to eq(1)
    end

    it "ignores nil fields" do
      data = [
        "foo", 
        ""
      ]
      
      # Create a pattern with nil validation for the first column
      pattern = [nil, nil]
      
      # Expect no errors
      validator = CsvUtils::Validator.new(pattern)
      expect(validator.validate_row(data)).to eq(true)

      expect(validator.status[:total_rows]).to eq(1)
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
      pattern = [:url, :url, nil, :protocol]

      # Expect no errors
      validator = CsvUtils::Validator.new(pattern)
      validator.validate_row(data)

      expect(validator.status[:total_rows]).to eq(1)
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
      pattern = [:url, :url, nil, :protocol]

      # Expect multiple errors
      validator = CsvUtils::Validator.new(pattern)
      expect(validator.validate_row(data)).to eq(false)

      expect(validator.status[:total_rows]).to eq(1)
      expect(validator.status[:failed_url_error_count]).to eq(1)
      expect(validator.status[:failed_protocol_error_count]).to eq(1)
    end
  end
end
