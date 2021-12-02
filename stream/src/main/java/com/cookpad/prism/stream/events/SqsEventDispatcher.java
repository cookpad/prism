package com.cookpad.prism.stream.events;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import com.cookpad.prism.StepHandler;

import io.sentry.Sentry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SqsEventDispatcher implements StepHandler {
    final private AmazonSQS sqs;
    final private String queueUrl;
    final private EventHandler eventHandler;
    final private Clock clock;

    @SuppressWarnings("serial")
    public static class ExtractException extends Exception {
        public ExtractException(Exception cause) {
            super(cause);
        }
    }

    private void handleMessage(Instant receiveTime, Message msg) throws ExtractException, EventHandler.CatchAndReleaseException {
        final String msgBody = msg.getBody();
        final S3EventNotification s3Event;
        final SnsEnvelope snsEnvelope;
        // At first, try parsing msgBody as a SNS Envelope.
        try {
            snsEnvelope = SnsEnvelope.parseJson(msgBody);
        } catch (IOException e) {
            throw new ExtractException(e);
        }

        final String s3EventMessage;
        if (snsEnvelope.getMessage() == null) {
            // Assume that message is bare if envelope's message is null
            s3EventMessage = msgBody;
        } else {
            s3EventMessage = snsEnvelope.getMessage();
        }

        try {
            s3Event = S3EventNotification.parseJson(s3EventMessage);
        } catch (SdkClientException e) {
            throw new ExtractException(e);
        }

        List<S3EventNotificationRecord> records = s3Event.getRecords();
        if (records == null) {
            // Just ignore test events which do not contain any records
            log.warn("S3Event.Records is empty");
            return;
        }
        for (S3EventNotificationRecord record: s3Event.getRecords()) {
            final Instant sendTime = Instant.ofEpochMilli(record.getEventTime().getMillis());
            final S3Entity s3Entity = record.getS3();
            final String bucketName = s3Entity.getBucket().getName();
            final String objectKey = s3Entity.getObject().getKey();
            StagingObjectEvent event = new StagingObjectEvent(bucketName, objectKey, sendTime, receiveTime);
            try {
                Sentry.getContext().addExtra("object_url", event.getObjectUri().toString());
                log.info("handle event: s3://{}/{}", bucketName, objectKey);
                this.eventHandler.handleEvent(event);
            } finally {
                Sentry.getContext().removeExtra("object_url");
            }
        }
    }

    private void receiveAndDispatch() {
        final ReceiveMessageRequest req = new ReceiveMessageRequest(this.queueUrl);
        req.setVisibilityTimeout(1200);
        req.setMaxNumberOfMessages(10);
        req.setWaitTimeSeconds(20);

        final ReceiveMessageResult msgResult = this.sqs.receiveMessage(req);
        final Instant receivedTime = Instant.now(clock);
        for (Message msg: msgResult.getMessages()) {
            try {
                this.handleMessage(receivedTime, msg);
            } catch (EventHandler.CatchAndReleaseException e) {
                // catch and release
                continue;
            } catch (Exception e) {
                log.error("Encountered an error in processing event message", e);
                continue;
            }
            DeleteMessageRequest delReq = new DeleteMessageRequest(this.queueUrl, msg.getReceiptHandle());
            this.sqs.deleteMessage(delReq);
        }
    }

    @Override
    public boolean handleStep() {
        receiveAndDispatch();
        return true;
    }

    @Override
    public void shutdown() {
        // FIXME: shutdown SQS client?
    }
}
