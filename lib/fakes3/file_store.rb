require 'fileutils'
require 'time'
require 'fakes3/s3_object'
require 'fakes3/bucket'
require 'fakes3/rate_limitable_file'
require 'digest/md5'
require 'yaml'
require 'sequel'

DB =
  begin
    ENV['DATABASE_URL'] ? Sequel.connect(ENV['DATABASE_URL']) : Sequel.sqlite('fakes3.db')
  rescue => e
    puts e.inspect
    raise e
  end

begin
  DB.create_table :items do
    primary_key :id
    String :bucket
    String :key
    String :metadata, text: true
    File :content
    DateTime :created_at
    DateTime :updated_at
    unique [:bucket, :key]
  end
rescue => e
  puts e.inspect
end
Sequel::Model.plugin :timestamps

class Item < Sequel::Model
  plugin :timestamps, update_on_create: true
end

module FakeS3
  class FileStore
    SHUCK_METADATA_DIR = ".fakes3_metadataFFF"
    # S3 clients with overly strict date parsing fails to parse ISO 8601 dates
    # without any sub second precision (e.g. jets3t v0.7.2), and the examples
    # given in the official AWS S3 documentation specify three (3) decimals for
    # sub second precision.
    SUBSECOND_PRECISION = 3

    def initialize(root)
      @root = root
      @buckets = []
      @bucket_hash = {}

      DB[:items].order(:bucket).each do |col|
        bucket_name = col['bucket']
        bucket_obj = Bucket.new(bucket_name,Time.now,[])
        @buckets << bucket_obj
        @bucket_hash[bucket_name] = bucket_obj
      end
    end

    # Pass a rate limit in bytes per second
    def rate_limit=(rate_limit)
      # :)
    end

    def buckets
      # :)
      []
    end

    def get_bucket(bucket)
      # :)
      Bucket.new(bucket, Time.now, [])
    end

    def create_bucket(bucket)
      # :)
      Bucket.new(bucket, Time.now, [])
    end

    def delete_bucket(bucket_name)
      # :)
    end

    def get_object(bucket,object_name, request)
      begin
        real_obj = S3Object.new
        item = Item.find(bucket: bucket.to_s, key: object_name.to_s)
        metadata = YAML.load(item.metadata)
        real_obj.name = object_name
        real_obj.md5 = metadata[:md5]
        real_obj.content_type = metadata.fetch(:content_type) { "application/octet-stream" }
        real_obj.item = item
        real_obj.size = metadata.fetch(:size) { 0 }
        real_obj.custom_metadata = metadata.fetch(:custom_metadata) { {} }
        return real_obj
      rescue
        puts $!
        $!.backtrace.each { |line| puts line }
        return nil
      end
    end

    def copy_object(src_bucket_name, src_name, dst_bucket_name, dst_name, request)
      src_item = Item.find(bucket: src_bucket_name, key: src_name)
      dst_item = Item.find(bucket: dst_bucket_name, key: dst_name) || Item.new(bucket: dst_bucket_name, key: dst_name)
      src_metadata = YAML::load(src_item.metadata)
      dst_item.metadata = src_item.metadata
      dst_item.content = src_item.content
      dst_item.save_changes

      src_bucket = get_bucket(src_bucket_name) || create_bucket(src_bucket_name)
      dst_bucket = get_bucket(dst_bucket_name) || create_bucket(dst_bucket_name)

      obj = S3Object.new
      obj.name = dst_name
      obj.md5 = src_metadata[:md5]
      obj.content_type = src_metadata[:content_type]
      obj.size = src_metadata[:size]
      obj.modified_date = src_metadata[:modified_date]
      return obj
    end

    def store_object(bucket, object_name, request)
      filedata = ""

      # TODO put a tmpfile here first and mv it over at the end
      content_type = request.content_type || ""

      match = content_type.match(/^multipart\/form-data; boundary=(.+)/)
      boundary = match[1] if match
      if boundary
        boundary  = WEBrick::HTTPUtils::dequote(boundary)
        form_data = WEBrick::HTTPUtils::parse_form_data(request.body, boundary)

        if form_data['file'] == nil or form_data['file'] == ""
          raise WEBrick::HTTPStatus::BadRequest
        end

        filedata = form_data['file']
      else
        request.body { |chunk| filedata << chunk }
      end

      do_store_object(bucket, object_name, filedata, request)
    end

    def do_store_object(bucket, object_name, filedata, request)
      begin
        metadata_struct = create_metadata(filedata, request)
        bucket_name = bucket.name
        item = Item.find(bucket: bucket_name, key: object_name) || Item.new(bucket: bucket_name, key: object_name)
        item.metadata = YAML::dump(metadata_struct)
        item.content = filedata
        item.save_changes

        obj = S3Object.new
        obj.name = object_name
        obj.md5 = metadata_struct[:md5]
        obj.content_type = metadata_struct[:content_type]
        obj.size = metadata_struct[:size]
        obj.modified_date = metadata_struct[:modified_date]

        bucket.add(obj)
        return obj
      rescue
        puts $!
        $!.backtrace.each { |line| puts line }
        return nil
      end
    end

    def combine_object_parts(bucket, upload_id, object_name, parts, request)
      raise 'not supported :('
    end

    def delete_object(bucket,object_name,request)
      begin
        item = Item.find(bucket: bucket.name, key: object_name)
        item.destroy
      rescue
        puts $!
        $!.backtrace.each { |line| puts line }
        return nil
      end
    end

    # TODO: abstract getting meta data from request.
    def create_metadata(file, request)
      metadata = {}
      metadata[:md5] = Digest::MD5.hexdigest(file)
      metadata[:content_type] = request.header["content-type"].first
      metadata[:size] = file.size
      metadata[:modified_date] = Time.now # TODO: FIXME
      metadata[:custom_metadata] = {}

      # Add custom metadata from the request header
      request.header.each do |key, value|
        match = /^x-amz-meta-(.*)$/.match(key)
        if match && (match_key = match[1])
          metadata[:custom_metadata][match_key] = value.join(', ')
        end
      end
      return metadata
    end
  end
end
