# vim: filetype=ruby
=begin
%%{
machine csv;

action value_start {
  register_start(p,[data[p]].pack("c*"),[data[p+1]].pack("c*"))
}

action value_end {
  register_end([data[p]].pack("c*"),data,p)
}

start_tag = ('T' | 'F' | '-' | digit | ';' | "'" | "#" | 's{' | 'v{' | 'm{') >value_start;
end_tag = (',' | '}') >value_end;
normal = (any - [\0\n%,] | '%00' | '%0A' | '%25' | '%2C' );
main := (start_tag | normal | end_tag) * . "\n" >value_end;

}%%
=end
module Hadoop
  class Csv
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
