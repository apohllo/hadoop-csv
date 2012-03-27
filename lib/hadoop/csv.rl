# vim: filetype=ruby
=begin
%%{
machine csv;

action value_start {
  register_start(p,data[p],data[p+1])
}

action value_end {
  register_end(data[p],data,p)
}

start_tag = ('T' | 'F' | '-' | digit | ';' | "'" | "#" | 's{' | 'v{' | 'm{') >value_start;
end_tag = (',' | '}') >value_end;
normal = (any - [\0\n%,] | '%00' | '%0A' | '%25' | '%2C' );
main := (start_tag | normal | end_tag) * . "\n" >value_end;

}%%
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
      %% write data;
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
      %% write init;
      # % (this fixes syntax highlighting)
      @result = [[]]
      @position = 0
      @states = [:default]
      data = line.unpack('c*')
      p = 0
      pe = data.length
      %% write exec;
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
