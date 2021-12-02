package com.cookpad.prism.stream;

import java.io.IOException;
import java.time.Clock;

import com.amazonaws.services.s3.AmazonS3URI;

import com.cookpad.prism.stream.events.EventHandler;
import com.cookpad.prism.stream.events.FileQueueEventDispatcher;
import com.cookpad.prism.stream.filequeue.FileQueue;
import com.cookpad.prism.stream.filequeue.S3QueueDownloader;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class FileQueueEventDispatcherFactory {
    private final S3QueueDownloader s3QueueDownloader;
    private final EventHandler eventHandler;
    private final Clock clock;

    public FileQueueEventDispatcher build(String queueFileUrl) throws IOException {
        AmazonS3URI uri = new AmazonS3URI(queueFileUrl);
        FileQueue fileQueue = s3QueueDownloader.download(uri);
        return new FileQueueEventDispatcher(fileQueue, eventHandler, clock);
    }
}
