package com.cookpad.prism.batch;

import java.time.Clock;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;

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
    public S3Client s3() {
        return S3Client.create();
    }

    @Bean
    public GlueClient glue() {
        return GlueClient.create();
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
