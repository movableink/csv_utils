require "csv_validator"
require "csv"

def collect_rows(sorter)
  rows = []
  sorter.each_batch(1) do |batch|
    batch.each do |item|
      rows << item[1]
    end
  end
  rows
end

RSpec.describe Sorter do
  it "should sort a CSV file" do
    sorter = Sorter.new([0], nil)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["4", "5", "6"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(2)
    expect(collect_rows(sorter)).to eq([["1", "2", "3"], ["4", "5", "6"]])
  end

  it "sorts a CSV file with compound keys" do
    sorter = Sorter.new([0, 1], nil)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["1", "3", "2"])
    sorter.add_row(["3", "1", "3"])
    sorter.add_row(["2", "3", "1"])
    sorter.add_row(["3", "1", "2"])
    sorter.add_row(["3", "2", "2"])
    sorter.add_row(["3", "2", "1"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(7)
    expect(collect_rows(sorter)).to eq(
      [
        ["3", "2", "2"], 
        ["3", "2", "1"], 
        ["1", "2", "3"], 
        ["1", "3", "2"], 
        ["3", "1", "3"], 
        ["3", "1", "2"], 
        ["2", "3", "1"]
      ]
    )
  end

  it "yields batches of sorted rows" do
    sorter = Sorter.new([0, 1], nil)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["4", "5", "6"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(2)
    count = 0
    sorter.each_batch(1) do |batch|
      expect(batch.size).to eq(1)
      count += 1
    end
    expect(count).to eq(2)
  end
end
