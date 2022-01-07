require 'aws-sdk-glue'
require 'aws-sdk-s3'
require 'json'

module Prism

  class GarbageCollector
    def initialize(s3:, glue:)
      @s3 = s3
      @glue = glue
    end

    def foreach_garbage_object(database_name, table_name, expression: nil)
      foreach_merged_partition(database_name, table_name, expression: expression) do |merged_partition|
        foreach_garbage_object_in_partition(merged_partition) do |m_obj|
          bucket = merged_partition.bucket
          key = "#{merged_partition.prefix}#{m_obj.to_basename}"
          object = S3Object.new(bucket: bucket, key: key)
          yield object
        end
      end
    end

    S3Object = Struct.new(:bucket, :key, keyword_init: true)
    class S3Object
      def to_s
        "s3://#{bucket}/#{key}"
      end
    end

    def foreach_merged_partition(database_name, table_name, expression: nil)
      foreach_glue_partitions(database_name, table_name, expression: expression) do |partition|
        # part is nil if not merged yet
        part = MergedPartition.try_from_location(partition.storage_descriptor.location)
        yield part if part
      end
    end

    def foreach_glue_partitions(database_name, table_name, expression: nil, &block)
      res = @glue.get_partitions(
        database_name: database_name,
        table_name: table_name,
        expression: expression
      )
      res.each do |page|
        page.partitions.each(&block)
      end
    end

    def foreach_garbage_object_in_partition(merged_partition)
      in_use = get_manifest_entries(merged_partition)
      greatest_endx_by_start_map = in_use.map {|m_obj| [m_obj.start, m_obj.endx] }.to_h
      foreach_merged_object(merged_partition) do |m_obj|
        greatest_endx = greatest_endx_by_start_map[m_obj.start]
        if greatest_endx && m_obj.endx < greatest_endx
          yield m_obj
        end
      end
    end

    def get_manifest_entries(merged_partition)
      manifest_body = @s3.get_object(bucket: merged_partition.bucket, key: "#{merged_partition.prefix}#{merged_partition.manifest_basename}").body.read
      manifest = JSON.parse(manifest_body, symbolize_names: true)
      manifest[:entries].map do |entry|
        entry_uri = URI.parse(entry[:url])
        entry_key = entry_uri.path[1..]
        entry_basename = File.basename(entry_key)
        MergedObject.parse_basename(entry_basename)
      end
    end

    def foreach_merged_object(merged_partition)
      res = @s3.list_objects_v2(
        bucket: merged_partition.bucket,
        delimiter: "/",
        prefix: "#{merged_partition.prefix}part-",
        max_keys: 1000,
      )
      res.each do |page|
        page.contents.each do |s3_obj|
          s3_obj_basename = File.basename(s3_obj.key)
          yield MergedObject.parse_basename(s3_obj_basename)
        end
      end
    end
  end

  class MergedPartition
    def MergedPartition.try_from_location(location)
      location_uri = URI.parse(location)
      unless location_uri.path.include?("/merged/")
        return nil
      end
      bucket = location_uri.host
      manifest_key = location_uri.path[1..]
      manifest_basename = File.basename(manifest_key)
      prefix = "#{File.dirname(manifest_key)}/"
      new(bucket, prefix, manifest_basename)
    end

    def initialize(bucket, prefix, manifest_basename)
      @bucket = bucket
      @prefix = prefix
      @manifest_basename = manifest_basename
    end

    attr_reader :prefix, :bucket, :manifest_basename
  end

  class MergedObject
    PART_BASENAME_RE = /part-(\d{19})-(\d{19})\.parquet/

    def MergedObject.parse_basename(basename)
      match_data = PART_BASENAME_RE.match(basename)
      if match_data.nil?
        raise "failed to parse merged object basename: #{basename}"
      end
      start = match_data[1].to_i
      endx = match_data[2].to_i
      new(start, endx)
    end

    # start: Integer
    # endx: Integer
    def initialize(start, endx)
      @start = start
      @endx = endx
    end

    attr_reader :start, :endx

    def to_basename
      "part-#{@start.to_s.rjust(19, '0')}-#{@endx.to_s.rjust(19, '0')}.parquet"
    end
  end

end
