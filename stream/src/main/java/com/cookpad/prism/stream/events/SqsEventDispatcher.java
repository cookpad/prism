package com.cookpad.prism.stream.events;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotificationRecord;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import com.cookpad.prism.StepHandler;

import io.sentry.Sentry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SqsEventDispatcher implements StepHandler {
    final private SqsClient sqs;
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
        final String msgBody = msg.body();
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
            s3Event = S3EventNotification.fromJson(s3EventMessage);
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
            final Instant sendTime = record.getEventTime();
            final String bucketName = record.getS3().getBucket().getName();
            final String objectKey = record.getS3().getObject().getKey();
            StagingObjectEvent event = new StagingObjectEvent(bucketName, objectKey, sendTime, receiveTime);
            try {
                Sentry.setExtra("object_url", event.getObjectUri().toString());
                log.info("handle event: s3://{}/{}", bucketName, objectKey);
                this.eventHandler.handleEvent(event);
            } finally {
                Sentry.configureScope(scope -> scope.removeExtra("object_url"));
            }
        }
    }

    private void receiveAndDispatch() {
        final ReceiveMessageRequest req = ReceiveMessageRequest.builder()
            .queueUrl(this.queueUrl)
            .visibilityTimeout(1200)
            .maxNumberOfMessages(10)
            .waitTimeSeconds(20)
            .build();

        final ReceiveMessageResponse msgResult = this.sqs.receiveMessage(req);
        final Instant receivedTime = Instant.now(clock);
        for (Message msg: msgResult.messages()) {
            try {
                this.handleMessage(receivedTime, msg);
            } catch (EventHandler.CatchAndReleaseException e) {
                // catch and release
                continue;
            } catch (Exception e) {
                log.error("Encountered an error in processing event message", e);
                continue;
            }
            DeleteMessageRequest delReq = DeleteMessageRequest.builder()
                .queueUrl(this.queueUrl)
                .receiptHandle(msg.receiptHandle())
                .build();
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
