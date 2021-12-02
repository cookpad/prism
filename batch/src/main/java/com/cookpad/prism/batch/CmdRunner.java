package com.cookpad.prism.batch;

import java.util.List;

import com.amazonaws.services.s3.AmazonS3URI;

import org.slf4j.MDC;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class CmdRunner implements CommandLineRunner {
    final private ApplicationContext ctx;
    private final ApplicationArguments args;

    private String getSingleOptionValue(String name) {
        List<String> values = this.args.getOptionValues(name);
        if (values.size() == 0) {
            throw new RuntimeException(String.format("No %s are given", name));
        }
        if (values.size() > 1) {
            throw new RuntimeException(String.format("Only one %s is allowed", name));
        }
        return values.get(0);
    }

    @Override
    public void run(String... rawArgs) throws Exception {
        List<String> nonopts = this.args.getNonOptionArgs();
        if (nonopts.size() < 1) {
            throw new RuntimeException("Command line args must be exactly one");
        }
        String cmdName = nonopts.get(0);
        MDC.put("cmd_name", cmdName);
        switch (cmdName) {
            case "CatalogCmd":
            case "sync":
                CatalogCmd catalogCmd = ctx.getBean(CatalogCmd.class);
                catalogCmd.run();
                break;
            case "ls-s3-objects":
                String destS3UriString = this.getSingleOptionValue("dest-s3-uri");
                AmazonS3URI destS3Uri = new AmazonS3URI(destS3UriString);
                String bucketName = this.getSingleOptionValue("bucket");
                String keyStartx = this.getSingleOptionValue("key-startx");
                String keyEndx = this.getSingleOptionValue("key-endx");
                ListStagingObjectsCmd listStagingObjectsCmd = ctx.getBean(ListStagingObjectsCmd.class);
                listStagingObjectsCmd.run(destS3Uri, bucketName, keyStartx, keyEndx);
                break;
            case "unlink-table":
                String tableIdString = this.getSingleOptionValue("table");
                int tableId = Integer.parseInt(tableIdString);
                UnlinkTableCmd unlinkTableCmd = ctx.getBean(UnlinkTableCmd.class);
                unlinkTableCmd.run(tableId);
                break;
            case "drop-table":
                String tableIdString2 = this.getSingleOptionValue("table");
                int tableId2 = Integer.parseInt(tableIdString2);
                DropTableCmd dropTableCmd = ctx.getBean(DropTableCmd.class);
                dropTableCmd.run(tableId2);
                break;
            default:
                throw new RuntimeException("No such cmd: " + cmdName);
        }
    }
}
