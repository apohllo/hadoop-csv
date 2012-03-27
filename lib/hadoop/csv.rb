
# line 1 "lib/hadoop/csv.rl"
# vim: filetype=ruby
=begin

# line 19 "lib/hadoop/csv.rl"

=end
module Hadoop
  class Csv
    attr_reader :path

    # Create new Hadoop CSV parser. If +path+ is given,
    # the file will be parsed in +each+ method.
    def initialize(path=nil)
      @path = path
      
# line 19 "lib/hadoop/csv.rb"
class << self
	attr_accessor :_csv_actions
	private :_csv_actions, :_csv_actions=
end
self._csv_actions = [
	0, 1, 0, 1, 1
]

class << self
	attr_accessor :_csv_key_offsets
	private :_csv_key_offsets, :_csv_key_offsets=
end
self._csv_key_offsets = [
	0, 0, 16, 18, 20, 22
]

class << self
	attr_accessor :_csv_trans_keys
	private :_csv_trans_keys, :_csv_trans_keys=
end
self._csv_trans_keys = [
	0, 10, 35, 37, 39, 44, 45, 59, 
	70, 84, 109, 115, 118, 125, 48, 57, 
	48, 50, 48, 65, 53, 67, 0
]

class << self
	attr_accessor :_csv_single_lengths
	private :_csv_single_lengths, :_csv_single_lengths=
end
self._csv_single_lengths = [
	0, 14, 2, 2, 2, 0
]

class << self
	attr_accessor :_csv_range_lengths
	private :_csv_range_lengths, :_csv_range_lengths=
end
self._csv_range_lengths = [
	0, 1, 0, 0, 0, 0
]

class << self
	attr_accessor :_csv_index_offsets
	private :_csv_index_offsets, :_csv_index_offsets=
end
self._csv_index_offsets = [
	0, 0, 16, 19, 22, 25
]

class << self
	attr_accessor :_csv_indicies
	private :_csv_indicies, :_csv_indicies=
end
self._csv_indicies = [
	1, 2, 3, 4, 3, 5, 3, 3, 
	3, 3, 3, 3, 3, 5, 3, 0, 
	6, 7, 1, 0, 0, 1, 0, 0, 
	1, 1, 0
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
	0, 0, 3, 1, 0, 3, 0, 0
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


# line 30 "lib/hadoop/csv.rl"
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

# line 53 "lib/hadoop/csv.rl"
      # % (this fixes syntax highlighting)
      @result = [[]]
      @position = 0
      @states = [:default]
      data = line.unpack('c*')
      p = 0
      pe = data.length
      
# line 156 "lib/hadoop/csv.rb"
begin
	_klen, _trans, _keys, _acts, _nacts = nil
	_goto_level = 0
	_resume = 10
	_eof_trans = 15
	_again = 20
	_test_eof = 30
	_out = 40
	while true
	_trigger_goto = false
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
	_keys = _csv_key_offsets[cs]
	_trans = _csv_index_offsets[cs]
	_klen = _csv_single_lengths[cs]
	_break_match = false
	
	begin
	  if _klen > 0
	     _lower = _keys
	     _upper = _keys + _klen - 1

	     loop do
	        break if _upper < _lower
	        _mid = _lower + ( (_upper - _lower) >> 1 )

	        if data[p].ord < _csv_trans_keys[_mid]
	           _upper = _mid - 1
	        elsif data[p].ord > _csv_trans_keys[_mid]
	           _lower = _mid + 1
	        else
	           _trans += (_mid - _keys)
	           _break_match = true
	           break
	        end
	     end # loop
	     break if _break_match
	     _keys += _klen
	     _trans += _klen
	  end
	  _klen = _csv_range_lengths[cs]
	  if _klen > 0
	     _lower = _keys
	     _upper = _keys + (_klen << 1) - 2
	     loop do
	        break if _upper < _lower
	        _mid = _lower + (((_upper-_lower) >> 1) & ~1)
	        if data[p].ord < _csv_trans_keys[_mid]
	          _upper = _mid - 2
	        elsif data[p].ord > _csv_trans_keys[_mid+1]
	          _lower = _mid + 2
	        else
	          _trans += ((_mid - _keys) >> 1)
	          _break_match = true
	          break
	        end
	     end # loop
	     break if _break_match
	     _trans += _klen
	  end
	end while false
	_trans = _csv_indicies[_trans]
	cs = _csv_trans_targs[_trans]
	if _csv_trans_actions[_trans] != 0
		_acts = _csv_trans_actions[_trans]
		_nacts = _csv_actions[_acts]
		_acts += 1
		while _nacts > 0
			_nacts -= 1
			_acts += 1
			case _csv_actions[_acts - 1]
when 0 then
# line 6 "lib/hadoop/csv.rl"
		begin

  register_start(p,[data[p]].pack("c*"),[data[p+1]].pack("c*"))
		end
when 1 then
# line 10 "lib/hadoop/csv.rl"
		begin

  register_end([data[p]].pack("c*"),data,p)
		end
# line 249 "lib/hadoop/csv.rb"
			end # action switch
		end
	end
	if _trigger_goto
		next
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

# line 61 "lib/hadoop/csv.rl"
      # % (this fixes syntax highlighting)
      @result[0]
    end

    protected
    def register_start(position,char,next_char)
      case @states.last
      when :default
        @position = position
        process_char(char,next_char)
      when :string
        # ignore
      when :bytes
        #ignore
      when :struct
        @position = position
        process_char(char,next_char)
      end
    end

    def process_char(char,next_char)
      case char
      when "'"
        @states << :string
      when "#"
        @states << :bytes
      when /s|v|m/
        if next_char == "{"
          @states << :struct
          @result << []
        end
      else
        @states << :other
      end
    end

    def register_end(char,data,position)
      #if char == "," || char == "}" #|| (@states.last != :string && @states.last != :bytes)
        last_start = @position
        new_data = data[last_start..position-1].pack("c*")
        case new_data
        when /^-?\d+\./
          @result.last << new_data.to_f
        when /^-?\d/
          @result.last << new_data.to_i
        when /^'/
          @result.last << new_data[1..-1].gsub(/%00/,"\0").gsub(/%0A/,"\n").
            gsub(/%25/,"%").gsub(/%2C/,",").force_encoding("utf-8")
        when /^T/
          @result.last << true
        when /^F/
          @result.last << false
        when /^}/
          subresult = @result.pop
          @result.last << subresult
        else
          raise "CSV error: #{new_data}"
        end
        @position = position
        @states.pop
      #end
    end
  end
end
