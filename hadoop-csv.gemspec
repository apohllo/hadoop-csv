$:.unshift "lib"

Gem::Specification.new do |s|
  s.name = "hadoop-csv"
  s.version = '0.0.3'
  s.date = "#{Time.now.strftime("%Y-%m-%d")}"
  s.authors = ['Aleksander Pohl']
  s.email   = ["apohllo@o2.pl"]
  #s.homepage    = "http://github.com/apohllo/rod"
  s.summary = "Hadoop CSV format parser."
  s.description = "Hadoop CSV format parser."

  s.rubyforge_project = "hadoop-csv"
  #s.rdoc_options = ["--main", "README.rdoc"]

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_path = "lib"
end
