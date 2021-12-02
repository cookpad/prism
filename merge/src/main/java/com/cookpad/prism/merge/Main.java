package com.cookpad.prism.merge;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import com.cookpad.prism.StepHandler;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.objectstore.PrismTableLocatorFactory;
import com.cookpad.prism.record.RecordReaderFactory;
import com.cookpad.prism.record.RecordWriterFactory;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@MapperScan(basePackages = "com.cookpad.prism.dao")
@Slf4j
public class Main {
    public static void main(String[] args) {
        try {
            new SpringApplicationBuilder(Main.class).web(WebApplicationType.NONE).run(args);
        } catch(Exception e) {
            log.error("Unhandled exception", e);
            throw e;
        }
    }

    @Bean
    public AmazonS3 s3() {
        return AmazonS3ClientBuilder.defaultClient();
    }

    @Bean
    public Configuration hadoopConf() {
        Configuration conf = new Configuration();
        conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
        return conf;
    }

    @Bean
    public RecordWriterFactory recordWriterFactory(@Autowired Configuration hadoopConf) {
        return new RecordWriterFactory(hadoopConf);
    }

    @Bean
    public RecordReaderFactory recordReaderFactory(@Autowired Configuration hadoopConf) {
        return new RecordReaderFactory(hadoopConf);
    }

    @Bean
    public PrismTableLocatorFactory tableLocatorFactory(@Autowired PrismMergeConf prismConf) {
        return new PrismTableLocatorFactory(prismConf.getBucketName(), prismConf.getPrefix());
    }

    @Bean
    public PrismObjectStoreFactory prismObjectStoreFactory(@Autowired PrismTableLocatorFactory tableLocatorFactory) {
        return new PrismObjectStoreFactory(this.s3(), tableLocatorFactory);
    }

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }

    @Bean
    public ParallelParquetMerger parallelParquetMerger(@Autowired PrismMergeConf prismMergeConf, @Autowired ParquetFileMerger parquetFileMerger, @Autowired PrismMergeConf prismConf) {
        ExecutorService downloadExecutor = Executors.newFixedThreadPool(prismConf.getDownloaderThreads(), DaemonThreadFactory.instance);
        ExecutorService mergeExecutor = Executors.newFixedThreadPool(prismConf.getMergerThreads(), DaemonThreadFactory.instance);
        return new ParallelParquetMerger(downloadExecutor, mergeExecutor, parquetFileMerger);
    }

    static class DaemonThreadFactory implements ThreadFactory {
        static final DaemonThreadFactory instance = new DaemonThreadFactory();

        final ThreadFactory original = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
            var th = original.newThread(r);
            th.setDaemon(true);
            return th;
        }
    }

    @Bean
    public StepHandler stepHandler(@Autowired MergeJobDispatcher mergeJobDispatcher) {
        return mergeJobDispatcher;
    }
}
