require 'prism/garbage_collector'
require 'bricolage/commandlineapplication'
require 'bricolage/logger'
require 'aws-sdk-glue'
require 'aws-sdk-s3'
require 'date'

module Prism

  class GarbageCollectApplication

    def GarbageCollectApplication.main
      new.main
    end

    DELETE_BATCH_SIZE = 50

    def main
      mode = :garbage_collect
      partition_expr = nil
      prism_tables = nil

      app = Bricolage::CommandLineApplication.define {|opts|
        opts.data_source_option('--prism-ds', 'Prism meta data data source', kind: 'sql', default: 'prism')
        opts.on('--list-tables', 'Shows target prism tables and quit.') {
          mode = :list_tables
        }
        opts.on('--table=SCHEMA_TABLE', 'Processes only this table.') {|table_spec|
          prism_tables = [PrismTable.parse(table_spec)]
        }
        opts.on('--list-objects', 'Shows garbage object list and quit.') {
          mode = :list_objects
        }
        opts.on('--partition-expr=EXPR', %Q(AWS Glue Catalog partition expression to filter target partitions.  e.g. "dt = '2021-03-15'")) {|expr|
          partition_expr = expr
        }
      }
      @logger = app.context.logger

      @logger.info "GC start"

      partition_expr ||= "dt = '#{default_target_date}'"
      @logger.info "partition_expr: #{partition_expr.inspect}"

      @logger.info "fetching prism table list..."
      prism_tables ||= dump_prism_tables(app.data_source('--prism-ds'))
      if mode == :list_tables
        prism_tables.each do |prism_table|
          puts "#{prism_table.id || '-'}\t#{prism_table.table_spec}"
        end
        exit 0
      end
      @logger.info "#{prism_tables.size} prism tables fetched"

      prism_bucket_name = ENV['PRISM_BUCKET_NAME']
      @logger.info "prism_bucket_name: #{prism_bucket_name}"

      s3 = Aws::S3::Client.new
      glue = Aws::Glue::Client.new
      gc = GarbageCollector.new(s3: s3, glue: glue)

      prism_tables.each do |prism_table|
        @logger.info "[#{prism_table.id || '?'}] #{prism_table.table_spec} ==================================================="
        if mode == :list_objects
          gc.foreach_garbage_object(prism_table.schema, prism_table.table, expression: partition_expr) do |object|
            puts object
          end
        else
          buf = []
          gc.foreach_garbage_object(prism_table.schema, prism_table.table, expression: partition_expr) do |object|
            @logger.info "[S3] DeleteObject #{object}"
            raise "is not prism bucket: #{object.bucket.inspect}" unless object.bucket == prism_bucket_name
            buf.push object.key

            if buf.size >= DELETE_BATCH_SIZE
              delete_objects(s3, prism_bucket_name, buf)
              buf = []
            end
          end
          unless buf.empty?
            delete_objects(s3, prism_bucket_name, buf)
          end
        end
      end

      @logger.info 'SUCCESS'
    end

    # NOTE: Set TZ environment to use local timezone partitions
    def default_target_date
      Date.today - 17
    end

    def dump_prism_tables(ds)
      ds.open {|conn|
        rows = conn.query_rows(<<~EndSQL)
          select
              t.prism_table_id
              , t.schema_name
              , t.table_name
          from
              prism_tables t
              inner join prism_tables_strload_streams ts using (prism_table_id)
              inner join strload_streams s using (stream_id)
          where
              not s.disabled
              and s.initialized
              and not s.discard
          order by 2, 3
        EndSQL

        rows.map {|row| PrismTable.for_row(row) }
      }
    end

    PrismTable = Struct.new(:id, :schema, :table, keyword_init: true)
    class PrismTable
      def PrismTable.parse(spec)
        s, t = spec.split('.', 2)
        new(schema: s, table: t)
      end

      def PrismTable.for_row(row)
        new(
          id: row['prism_table_id'].to_i,
          schema: row['schema_name'],
          table: row['table_name']
        )
      end

      def table_spec
        "#{schema}.#{table}"
      end
    end

    def delete_objects(s3, bucket_name, keys)
      res = s3.delete_objects({
        bucket: bucket_name,
        delete: {
          objects: keys.map {|k| {key: k} },
          #quiet: true
        }
      })
      if res.errors.empty?
        @logger.info "[S3] DeleteObjects: #{keys.size} objects deleted"
      else
        res.errors.each do |err|
          @logger.error "#{err.code} #{err.message}: #{err.key}"
        end
        raise "DeleteObjects failed"
      end
    end

  end

end
