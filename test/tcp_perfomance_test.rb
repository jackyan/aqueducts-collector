#!/usr/bin/env ruby
require 'socket'

workers = 12
batch_num = 500000

def make_test_data(batch_num)
  s = TCPSocket.new 'tc-aqueducts-dev00.tc', 8089
  batch_num.times do |n|
    s.write("{\"product\" : \"nodejs\", \"service\" : \"kafka\", \"idc\" : \"tc\", \"page_view\" : \"#{n}\", \"response_time\" : \"#{n}\", \"src_prod\" : \"test\", \"lidc\" : \"HD\"}\n")
    sleep 0.002
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
