package com.cookpad.prism.merge;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class CmdRunner implements CommandLineRunner {
    private final ApplicationContext ctx;
    private final ApplicationArguments args;

    @Override
    public void run(String... rawArgs) throws Exception {
        List<String> nonopts = this.args.getNonOptionArgs();
        String cmdName = "daemon";
        if (nonopts.size() >= 1) {
            cmdName = nonopts.get(0);
        }
        MDC.put("cmd_name", cmdName);
        switch (cmdName) {
            case "daemon":
                DaemonCmd daemonCmd = this.ctx.getBean(DaemonCmd.class);
                daemonCmd.run();
                break;
            case "oneshot":
                OneshotCmd oneshotCmd = this.ctx.getBean(OneshotCmd.class);
                oneshotCmd.run();
                break;
            case "rebuild":
                List<String> tableIds = this.args.getOptionValues("table");
                if (tableIds.size() != 1) {
                    throw new RuntimeException("just one table id must be given");
                }
                int tableId = Integer.parseInt(tableIds.get(0));
                List<String> partitions = this.args.getOptionValues("partition");
                List<LocalDate> partitionDates =  partitions.stream().map(LocalDate::parse).collect(Collectors.toList());
                RebuildCmd rebuildCmd = this.ctx.getBean(RebuildCmd.class);
                rebuildCmd.run(tableId, partitionDates);
                break;
            default:
                throw new RuntimeException("No such cmd: " + cmdName);
        }
    }
}
