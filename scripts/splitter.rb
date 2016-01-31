#!/usr/bin/ruby
require 'csv'

DEFAULT_CLUSTER = {

  size: 0
}

filepath = File.expand_path(File.join(File.dirname(__FILE__), ARGV[0]))
centroids = CSV.read(filepath)
  .group_by {|r| r.last }
  .reduce(Hash.new) {|clusters, (id, points)|
    x, y = points.reduce([0, 0]) {|(a, b), (x, y)| [a + x.to_f, b + y.to_f] }
    clusters[id.to_i] = [x / points.size, y / points.size]
    clusters
  }

centroids.delete(0)
centroids.values.each do |centroid|
  puts centroid.map(&:to_s).join("\t")
end
