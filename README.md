# CsvUtils

A high-performance CSV processing library for Ruby, providing efficient sorting, validation, and batch processing capabilities.

## Features

- **Efficient CSV Sorting**: Sort large CSV files with minimal memory usage using external merge sort
- **URL Validation**: Built-in validation for URL fields
- **Protocol Validation**: Validate protocol presence in fields
- **Batch Processing**: Process CSV data in configurable batch sizes
- **Memory Management**: Configurable buffer sizes for optimal memory usage
- **Error Tracking**: Detailed error reporting for validation failures

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'csv_utils', git: "https://github.com/movableink/csv_validator"
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install csv_utils
```

## Usage

### Basic Sorting

```ruby
require 'csv_utils'

# Create a new sorter
sorter = CsvUtils::Sorter.new("my_source", [0, 1], 100)
)

# Add rows
sorter.add_row(["value1", "value2", "url1"])
sorter.add_row(["value3", "value4", "url2"])

# Sort and get results
result = sorter.sort!
puts "Total rows processed: #{result['total_rows']}"

# Read back the result in batches
sorter.each_batch(1000) do |batch|
  batch.each do |row|
    # Process each row
  end
end
```

### Validation

```ruby
# Set validation schema
sorter.set_validation_schema([:url, :protocol, nil])  # nil means ignore column

# Process rows with validation
sorter.add_row(["https://example.com", "http://", "ignored"])
```

## Development

After checking out the repo, run `bundle` to install dependencies. Then, run `bundle exec rake compile` to build the native code. Then run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/movableink/csv_utils.

## License

Copyright 2025 Movable, Inc.