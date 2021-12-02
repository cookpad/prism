package com.cookpad.prism.stream.events;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;

import com.amazonaws.services.s3.AmazonS3URI;

import com.cookpad.prism.StepHandler;
import com.cookpad.prism.stream.events.EventHandler.CatchAndReleaseException;
import com.cookpad.prism.stream.filequeue.FileQueue;

import io.sentry.Sentry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class FileQueueEventDispatcher implements StepHandler {
    private final FileQueue fileQueue;
    private final EventHandler eventHandler;
    private final Clock clock;

    @Override
    public boolean handleStep() {
        while (true) {
            try {
                String s3UrlString = this.fileQueue.peek();
                if (s3UrlString == null) {
                    break;
                }
                final AmazonS3URI s3Url = new AmazonS3URI(s3UrlString);
                final Instant receiveTime = Instant.now(clock);
                final Instant sendTime = receiveTime;
                final StagingObjectEvent event = new StagingObjectEvent(s3Url.getBucket(), s3Url.getKey(), sendTime, receiveTime);
                eventHandler.handleEvent(event);
                this.fileQueue.dequeue();
            } catch (CatchAndReleaseException e) {
                // catch and release
                continue;
            } catch (IOException e) {
                log.error(String.format("Encountered an error at line:%d", this.fileQueue.lineNumber()), e);
                continue;
            }
        }
        return false;
    }

    @Override
    public void shutdown() {
    }
}
