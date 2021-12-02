package com.cookpad.prism.batch;

import java.time.Clock;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.cookpad.prism.batch.catalog.DatabaseNameModifier;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.objectstore.PrismTableLocatorFactory;
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
    public AWSGlue glue() {
        return AWSGlueClientBuilder.defaultClient();
    }

    @Bean
    public PrismTableLocatorFactory tableLocatorFactory(@Autowired PrismBatchConf prismConf) {
        return new PrismTableLocatorFactory(prismConf.getBucketName(), prismConf.getPrefix());
    }

    @Bean
    public PrismObjectStoreFactory prismObjectStoreFactory(@Autowired PrismTableLocatorFactory tableLocatorFactory) {
        return new PrismObjectStoreFactory(this.s3(), tableLocatorFactory);
    }

    @Bean
    public DatabaseNameModifier databaseNameModifier(@Autowired PrismBatchConf prismConf) {
        String prefix = prismConf.getCatalog().getDatabasePrefix();
        String suffix = prismConf.getCatalog().getDatabaseSuffix();
        return new DatabaseNameModifier(prefix, suffix);
    }

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}
