package com.cookpad.prism.stream.events;

import com.cookpad.prism.stream.events.EventHandler.CatchAndReleaseException;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import lombok.val;

public class SqsEventDispatcherTest {
    @Test
    void testHandleStepWithSNS() throws CatchAndReleaseException {
        val now = Instant.now();
        val mockedClock = mock(Clock.class);
        when(mockedClock.instant()).thenReturn(now);
        val mockedSqs = mock(SqsClient.class);
        val messageBody = "{\"Message\":\"{\\\"Records\\\":[{\\\"eventVersion\\\":\\\"2.0\\\",\\\"eventTime\\\":\\\"2018-06-27T11:24:59.461Z\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"ap-northeast-1\\\",\\\"eventName\\\":\\\"ObjectCreated:Put\\\",\\\"s3\\\":{\\\"bucket\\\":{\\\"name\\\":\\\"staging-bucket\\\"},\\\"object\\\":{\\\"key\\\":\\\"69ab.logs.pv_log/2018/06/27/20180627_1124_0_4ee44954-228b-4f08-a832-360c625f4e92.gz\\\"}}}]}\",\"Timestamp\":\"2018-06-27T11:24:59.461Z\"}";
        val result = ReceiveMessageResponse.builder()
            .messages(
                Message.builder()
                    .receiptHandle("DUMMY_RECEIPT_HANDLE")
                    .body(messageBody)
                    .build()
            )
            .build();
        when(mockedSqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);

        val mockedHandler = mock(EventHandler.class);
        doNothing().when(mockedHandler).handleEvent(any());

        val dispatcher = new SqsEventDispatcher(mockedSqs, "dummy", mockedHandler, mockedClock);
        dispatcher.handleStep();

        verify(mockedHandler).handleEvent(
            new StagingObjectEvent(
                "staging-bucket",
                "69ab.logs.pv_log/2018/06/27/20180627_1124_0_4ee44954-228b-4f08-a832-360c625f4e92.gz",
                ZonedDateTime.parse("2018-06-27T11:24:59.461Z").toInstant(),
                now
            )
        );
    }

    @Test
    void testHandleStepWithoutSNS() throws CatchAndReleaseException {
        val now = Instant.now();
        val mockedClock = mock(Clock.class);
        when(mockedClock.instant()).thenReturn(now);
        val mockedSqs = mock(SqsClient.class);
        val messageBody = "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventTime\":\"2018-06-27T11:24:59.461Z\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"ap-northeast-1\",\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"bucket\":{\"name\":\"staging-bucket\"},\"object\":{\"key\":\"69ab.logs.pv_log/2018/06/27/20180627_1124_0_4ee44954-228b-4f08-a832-360c625f4e92.gz\"}}}]}";
        val result = ReceiveMessageResponse.builder()
            .messages(
                Message.builder()
                    .receiptHandle("DUMMY_RECEIPT_HANDLE")
                    .body(messageBody)
                    .build()
            )
            .build();
        when(mockedSqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);

        val mockedHandler = mock(EventHandler.class);
        doNothing().when(mockedHandler).handleEvent(any());

        val dispatcher = new SqsEventDispatcher(mockedSqs, "dummy", mockedHandler, mockedClock);
        dispatcher.handleStep();

        verify(mockedHandler).handleEvent(
            new StagingObjectEvent(
                "staging-bucket",
                "69ab.logs.pv_log/2018/06/27/20180627_1124_0_4ee44954-228b-4f08-a832-360c625f4e92.gz",
                ZonedDateTime.parse("2018-06-27T11:24:59.461Z").toInstant(),
                now
            )
        );
    }
}
