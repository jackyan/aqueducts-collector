#!/usr/bin/env ruby

require './relp.rb'

workers = 12
batch_num = 500000

def make_test_data(batch_num)
  s = RelpClient.new('tc-aqueducts-dev00.tc', 8091, ['syslog'], 256, 1)
  batch_num.times do |n|
    s.syslog_write("{\"product\" : \"nodejs\", \"service\" : \"kafka\", \"idc\" : \"tc\", \"page_view\" : \"#{n}\", \"response_time\" : \"#{n}\", \"src_prod\" : \"test\", \"lidc\" : \"HD\"}\n")
    sleep 0.001
  end
  s.close
end

start = (Time.now.to_f * 1000).to_i
threads = []
workers.times do |n|
  threads << Thread.new { make_test_data(batch_num) }
end
threads.each {|t| t.join }
puts "send : " + (workers * batch_num).to_s + " messages in : " + ((Time.now.to_f * 1000).to_i - start).to_s
