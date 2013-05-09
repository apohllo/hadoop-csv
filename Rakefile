desc "Generate the Ruby state machine"
task :generate do
  sh "ragel -T1 -F1 -R lib/hadoop/csv.rl"
end

desc "Run integration tests"
task :tests do
  sh "ruby test/integration.rb"
end
