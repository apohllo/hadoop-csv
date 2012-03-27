
# line 1 "lib/hadoop/csv.rl"
# vim: filetype=ruby
=begin

# line 19 "lib/hadoop/csv.rl"

=end
module Hadoop
  class Csv
    SINGLE_QUOTE_CODE = "'".codepoints.first
    DASH_CODE = "#".codepoints.first
    S_CODE = "s".codepoints.first
    V_CODE = "v".codepoints.first
    M_CODE = "m".codepoints.first
    OPENING_BRACE_CODE = "{".codepoints.first

    attr_reader :path

    # Create new Hadoop CSV parser. If +path+ is given,
    # the file will be parsed in +each+ method.
    def initialize(path=nil)
      @path = path
      
# line 26 "lib/hadoop/csv.rb"
class << self
	attr_accessor :_csv_trans_keys
	private :_csv_trans_keys, :_csv_trans_keys=
end
self._csv_trans_keys = [
	0, 0, 0, 125, 48, 50, 
	48, 65, 53, 67, 0, 
	0, 0
]

class << self
	attr_accessor :_csv_key_spans
	private :_csv_key_spans, :_csv_key_spans=
end
self._csv_key_spans = [
	0, 126, 3, 18, 15, 0
]

class << self
	attr_accessor :_csv_index_offsets
	private :_csv_index_offsets, :_csv_index_offsets=
end
self._csv_index_offsets = [
	0, 0, 127, 131, 150, 166
]

class << self
	attr_accessor :_csv_indicies
	private :_csv_indicies, :_csv_indicies=
end
self._csv_indicies = [
	1, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 2, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 3, 0, 4, 0, 3, 
	0, 0, 0, 0, 5, 3, 0, 0, 
	3, 3, 3, 3, 3, 3, 3, 3, 
	3, 3, 0, 3, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 3, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 3, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 3, 0, 0, 
	0, 0, 0, 3, 0, 0, 3, 0, 
	0, 0, 0, 0, 0, 5, 0, 6, 
	1, 7, 1, 0, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 0, 1, 0, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 0, 1, 1, 0
]

class << self
	attr_accessor :_csv_trans_targs
	private :_csv_trans_targs, :_csv_trans_targs=
end
self._csv_trans_targs = [
	1, 0, 5, 1, 2, 1, 3, 4
]

class << self
	attr_accessor :_csv_trans_actions
	private :_csv_trans_actions, :_csv_trans_actions=
end
self._csv_trans_actions = [
	0, 0, 1, 2, 0, 1, 0, 0
]

class << self
	attr_accessor :csv_start
end
self.csv_start = 1;
class << self
	attr_accessor :csv_first_final
end
self.csv_first_final = 5;
class << self
	attr_accessor :csv_error
end
self.csv_error = 0;

class << self
	attr_accessor :csv_en_main
end
self.csv_en_main = 1;


# line 37 "lib/hadoop/csv.rl"
      # % (this fixes syntax highlighting)
    end

    # Opens the file given in constructor and yields
    # the parsed results.
    def each
      if block_given?
        File.open(path) do |f|
          while !f.eof? && line = f.readline
            yield parse(line)
          end
        end
      else
        enum_for(:each)
      end
    end

    # Parse single line of Hadoop CSV. The line must end with '\n'.
    def parse(line)
      # So that ragel doesn't try to get it from data.length
      pe = :ignored
      eof = :ignored
      
# line 140 "lib/hadoop/csv.rb"
begin
	p ||= 0
	pe ||= data.length
	cs = csv_start
end

# line 60 "lib/hadoop/csv.rl"
      # % (this fixes syntax highlighting)
      @result = [[]]
      @position = 0
      @states = [:default]
      data = line.unpack('c*')
      p = 0
      pe = data.length
      
# line 156 "lib/hadoop/csv.rb"
begin
	testEof = false
	_slen, _trans, _keys, _inds, _acts, _nacts = nil
	_goto_level = 0
	_resume = 10
	_eof_trans = 15
	_again = 20
	_test_eof = 30
	_out = 40
	while true
	if _goto_level <= 0
	if p == pe
		_goto_level = _test_eof
		next
	end
	if cs == 0
		_goto_level = _out
		next
	end
	end
	if _goto_level <= _resume
	_keys = cs << 1
	_inds = _csv_index_offsets[cs]
	_slen = _csv_key_spans[cs]
	_trans = if (   _slen > 0 && 
			_csv_trans_keys[_keys] <= data[p].ord && 
			data[p].ord <= _csv_trans_keys[_keys + 1] 
		    ) then
			_csv_indicies[ _inds + data[p].ord - _csv_trans_keys[_keys] ] 
		 else 
			_csv_indicies[ _inds + _slen ]
		 end
	cs = _csv_trans_targs[_trans]
	if _csv_trans_actions[_trans] != 0
	case _csv_trans_actions[_trans]
	when 2 then
# line 6 "lib/hadoop/csv.rl"
		begin

  register_start(p,data[p],data[p+1])
		end
	when 1 then
# line 10 "lib/hadoop/csv.rl"
		begin

  register_end(data[p],data,p)
		end
# line 204 "lib/hadoop/csv.rb"
	end
	end
	end
	if _goto_level <= _again
	if cs == 0
		_goto_level = _out
		next
	end
	p += 1
	if p != pe
		_goto_level = _resume
		next
	end
	end
	if _goto_level <= _test_eof
	end
	if _goto_level <= _out
		break
	end
end
	end

# line 68 "lib/hadoop/csv.rl"
      # % (this fixes syntax highlighting)
      @result[0]
    end

    protected
    def register_start(position,char_code,next_char_code)
      case @states.last
      when :default
        @position = position
        process_char(char_code,next_char_code)
      when :string
        # ignore
      when :bytes
        #ignore
      when :struct
        @position = position
        process_char(char_code,next_char_code)
      end
    end

    def process_char(char_code,next_char_code)
      case char_code
      when SINGLE_QUOTE_CODE
        @states << :string
      when DASH_CODE
        @states << :bytes
      when S_CODE, V_CODE, M_CODE
        if next_char_code == OPENING_BRACE_CODE
          @states << :struct
          @result << []
        end
      else
        @states << :other
      end
    end

    def register_end(char_code,data,position)
      # TODO there seems to be ambiguity in the CSV format:
      # unicode string/byte sequence containing the closing brace
      # TODO fix char -> char_code
      #if char == "," || char == "}" #|| (@states.last != :string && @states.last != :bytes)
      last_start = @position
      new_data = data[last_start..position-1].pack("c*")
      case new_data[0]
      when "'"
        @result.last << new_data[1..-1].gsub(/%00/,"\0").gsub(/%0A/,"\n").
          gsub(/%25/,"%").gsub(/%2C/,",").force_encoding("utf-8")
      when "T","F"
        if new_data == "T"
          @result.last << true
        else
          @result.last << false
        end
      when "}"
        subresult = @result.pop
        @result.last << subresult
      else
        if new_data =~ /^-?\d+(\.)?/
          if $~[1].nil?
            @result.last << new_data.to_i
          else
            @result.last << new_data.to_f
          end
        else
          raise "CSV error: #{new_data}"
        end
      end
      @position = position
      @states.pop
    end
  end
end
