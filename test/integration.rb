require_relative '../lib/hadoop/csv'
require 'minitest/autorun'

DATA = [
  "493001,v{s{'szczotecznica szarawka,5,5,F,F,T},s{'szczotecznicy szarawki,1,1,F,F,T},s{'Calliteara pudibunda,0,0,T,F,T},s{'Dasychira pudibunda,0,0,T,F,T},s{'Szczotecznica szarawka,0,0,F,T,T}}\n",
  "39677,v{s{'trzcinniczek,19,19,F,F,T},s{'trzcinniczka,5,5,F,F,T},s{'trzcinniczek zwyczajny,4,4,F,F,T},s{'trzcinniczka zwyczajnego,2,2,F,F,T},s{'trzcinniczki,2,2,F,F,T},s{'Trzcinniczek zwyczajny,1,1,F,T,T},s{'Trzcinniczek,1,1,F,F,T},s{'%7Dtrzcinniczek,1,1,F,F,T},s{'trzciniak,1,1,F,F,F},s{'Acrocephalus scirpaceus,0,0,T,F,T}}\n"
]

describe "Hadoop::CSV" do
  before { @subject = Hadoop::Csv.new }

  it "parses a number" do
    parsed = @subject.parse(DATA[0])[0]
    parsed.must_equal 493001
  end

  it "parses a vector" do
    parsed = @subject.parse(DATA[0])[1]
    parsed.size.must_equal 5
    parsed.respond_to?(:each).must_equal true
  end

  it "parses a structure" do
    parsed = @subject.parse(DATA[0])[1][0]
    parsed.size.must_equal 6
  end

  it "parses a string" do
    parsed = @subject.parse(DATA[0])[1][0][0]
    parsed.must_equal "szczotecznica szarawka"
  end

  it "parses True value" do
    parsed = @subject.parse(DATA[0])[1][0][-1]
    parsed.must_equal true
  end

  it "parses line with special characters" do
    parsed = @subject.parse(DATA[1])[1][7][0]
    parsed.must_equal "}trzcinniczek"
  end
end
