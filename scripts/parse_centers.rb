#!/usr/bin/ruby
puts ARGV.first.split(', ').map {|a| (eval a).map(&:to_i).join(%{\t}) }
