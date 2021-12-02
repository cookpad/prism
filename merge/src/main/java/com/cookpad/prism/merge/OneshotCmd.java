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
public class OneshotCmd {
    final StepHandler stepHandler;

    public void run() {
        try {
            var succeeded = this.stepHandler.handleStep();
            log.info(succeeded ? "suceeded" : "failed");
        }
        finally {
            log.info("shutting down job dispatcher...");
            this.stepHandler.shutdown();
            log.info("shut down.");
        }
    }
}
