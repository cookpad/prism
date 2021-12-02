package com.cookpad.prism.stream;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import com.cookpad.prism.Banner;
import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.StepHandler;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.objectstore.PrismTableLocatorFactory;
import com.cookpad.prism.objectstore.StagingObjectStore;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.stream.events.DateRange;
import com.cookpad.prism.stream.events.EventHandler;
import com.cookpad.prism.stream.events.SqsEventDispatcher;
import com.cookpad.prism.stream.events.StagingObjectDispatcher;
import com.cookpad.prism.stream.events.StagingObjectHandler;
import com.cookpad.prism.stream.filequeue.S3QueueDownloader;
import com.cookpad.prism.dao.PacketStreamMapper;
import com.cookpad.prism.dao.PrismStagingObjectMapper;
import com.cookpad.prism.dao.PrismUnknownStagingObjectMapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@MapperScan(basePackages = "com.cookpad.prism.dao")
@Slf4j
public class Main {
    public static void main(String[] args) {
        try {
            new SpringApplicationBuilder(Main.class).web(WebApplicationType.NONE).run(args);
        } catch(Throwable e) {
            log.error("Unhandled exception", e);
            throw e;
        }
    }

    @Bean
    public AmazonSQS sqs() {
        return AmazonSQSClientBuilder.defaultClient();
    }

    @Bean
    public AmazonS3 s3() {
        return AmazonS3ClientBuilder.defaultClient();
    }

    @Bean
    public PrismTableLocatorFactory tableLocatorFactory(@Autowired PrismStreamConf prismConf) {
        return new PrismTableLocatorFactory(prismConf.getBucketName(), prismConf.getPrefix());
    }

    @Bean
    public PrismObjectStoreFactory prismObjectStoreFactory(@Autowired AmazonS3 s3, @Autowired PrismStreamConf prismConf, @Autowired PrismTableLocatorFactory tablePrefixer) {
        return new PrismObjectStoreFactory(s3, tablePrefixer);
    }

    @Bean
    public StagingObjectStore stagingObjectStore(@Autowired AmazonS3 s3) {
        return new StagingObjectStore(s3);
    }

    @Bean
    public SchemaBuilder schemaBuilder() {
        return new SchemaBuilder();
    }

    @Bean
    public SqsEventDispatcher sqsEventDispatcher(@Autowired AmazonSQS sqs, @Autowired EventHandler eventHandler, @Autowired PrismStreamConf prismConf) {
        return new SqsEventDispatcher(sqs, prismConf.getQueueUrl(), eventHandler, Clock.systemDefaultZone());
    }

    @Bean
    public S3QueueDownloader s3QueueDownloader(@Autowired AmazonS3 s3) {
        return new S3QueueDownloader(s3);
    }

    @Bean
    public StepHandler stepHandler(@Autowired ApplicationContext ctx, @Autowired ApplicationArguments args)
            throws IOException {
        List<String> queueFileUrls = args.getOptionValues("queue-file-url");
        if (queueFileUrls == null || queueFileUrls.size() == 0) {
            return ctx.getBean(SqsEventDispatcher.class);
        }
        if (queueFileUrls.size() != 1) {
            throw new RuntimeException("multiple queue-file-url are given");
        }
        FileQueueEventDispatcherFactory factory = ctx.getBean(FileQueueEventDispatcherFactory.class);
        return factory.build(queueFileUrls.get(0));
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
    public Clock clock() {
        return Clock.systemDefaultZone();
    }

    @Bean StagingObjectDispatcher stagingObjectDispatcher(
        @Autowired StagingObjectHandler stagingObjectHandler,
        @Autowired PrismStagingObjectMapper stagingObjectMapper,
        @Autowired PrismUnknownStagingObjectMapper unknownStagingObjectMapper,
        @Autowired PacketStreamMapper packetStreamMapper,
        @Autowired PrismStreamConf prismConf
    ) {
        DateRange ignoreDateRange = new DateRange(
            Optional.ofNullable(prismConf.getIgnoreFromExclusive()).map(LocalDate::parse),
            Optional.ofNullable(prismConf.getIgnoreToInclusive()).map(LocalDate::parse));
        boolean isIn = ignoreDateRange.contains(LocalDate.of(2018, 10, 1));
        System.out.println(isIn);
        return new StagingObjectDispatcher(stagingObjectHandler, stagingObjectMapper, unknownStagingObjectMapper, packetStreamMapper, ignoreDateRange);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx, StepHandler stepHandler) {
        return args -> {
            System.out.println(Banner.getBanner());
            System.out.println("It's the Prism S.");
            while (stepHandler.handleStep()) { }
        };
    }
}
