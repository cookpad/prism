package com.cookpad.prism.merge;

import com.cookpad.prism.StepHandler;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Lazy
@RequiredArgsConstructor
@Slf4j
public class DaemonCmd {
    private final StepHandler stepHandler;

    public void run() {
        try {
            while (this.stepHandler.handleStep()) {
                ;
            }
        }
        finally {
            log.info("shutting down job dispatcher...");
            this.stepHandler.shutdown();
            log.info("shut down.");
        }
    }
}
