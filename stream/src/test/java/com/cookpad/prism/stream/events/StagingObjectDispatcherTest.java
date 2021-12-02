package com.cookpad.prism.stream.events;

import com.cookpad.prism.stream.StagingObjectAttributes;
import com.cookpad.prism.stream.events.EventHandler.CatchAndReleaseException;
import com.cookpad.prism.stream.events.StagingObjectHandler.UnknownObjectException;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.OneToOne;
import com.cookpad.prism.dao.PacketStream;
import com.cookpad.prism.dao.PacketStreamMapper;
import com.cookpad.prism.dao.PrismStagingObject;
import com.cookpad.prism.dao.PrismStagingObjectMapper;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismUnknownStagingObjectMapper;
import com.cookpad.prism.dao.StreamColumn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

public class StagingObjectDispatcherTest {
    @Test
    void testHandleEvent() throws UnknownObjectException, CatchAndReleaseException {
        var stagingObjectHandler = mock(StagingObjectHandler.class);
        var stagingObjectMapper = mock(PrismStagingObjectMapper.class);
        var stagingObject = mock(PrismStagingObject.class);
        when(stagingObjectMapper.findByBucketNameAndObjectKey("dest_bucket", "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change/2018/07/19/20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz")).thenReturn(stagingObject);
        var packetStream = mock(PacketStream.class);
        when(packetStream.isDisabled()).thenReturn(false);
        when(packetStream.isDiscard()).thenReturn(false);
        when(packetStream.isInitialized()).thenReturn(true);
        var streamColumns = List.of(mock(StreamColumn.class));
        var prismTable = mock(PrismTable.class);
        var packetStreamWithColumns = new OneToMany<>(packetStream, streamColumns);
        var row = new OneToOne<>(packetStreamWithColumns, prismTable);
        var packetStreamMapper = mock(PacketStreamMapper.class);
        when(packetStreamMapper.findByDestBucketAndPrefix("dest_bucket", "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change")).thenReturn(List.of(row));
        var unknownStagingObjectMapper = mock(PrismUnknownStagingObjectMapper.class);
        var ignoreDateRange = mock(DateRange.class);
        when(ignoreDateRange.contains(any())).thenReturn(false);
        var dispatcher = new StagingObjectDispatcher(stagingObjectHandler, stagingObjectMapper, unknownStagingObjectMapper, packetStreamMapper, ignoreDateRange);
        var stagingObjectEvent = new StagingObjectEvent("dest_bucket", "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change/2018/07/19/20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz", Instant.now(), Instant.now());
        dispatcher.handleEvent(stagingObjectEvent);
        var expectedAttrs = new StagingObjectAttributes(
            "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change",
            "hako_console.hako_console_autoscale_limit_change",
            LocalDate.of(2018, 7, 19),
            "20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz");
        verify(stagingObjectHandler).handleStagingObject(stagingObject, expectedAttrs, packetStreamWithColumns, prismTable);
    }

    @Test
    void testHandleEventIgnoreDateRange() throws UnknownObjectException, CatchAndReleaseException {
        var stagingObjectHandler = mock(StagingObjectHandler.class);
        var stagingObjectMapper = mock(PrismStagingObjectMapper.class);
        var stagingObject = mock(PrismStagingObject.class);
        when(stagingObjectMapper.findByBucketNameAndObjectKey("dest_bucket", "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change/2018/07/19/20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz")).thenReturn(stagingObject);
        var packetStream = mock(PacketStream.class);
        when(packetStream.isDisabled()).thenReturn(false);
        when(packetStream.isDiscard()).thenReturn(false);
        when(packetStream.isInitialized()).thenReturn(true);
        var streamColumns = List.of(mock(StreamColumn.class));
        var prismTable = mock(PrismTable.class);
        var packetStreamWithColumns = new OneToMany<>(packetStream, streamColumns);
        var row = new OneToOne<>(packetStreamWithColumns, prismTable);
        var packetStreamMapper = mock(PacketStreamMapper.class);
        when(packetStreamMapper.findByDestBucketAndPrefix("dest_bucket", "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change")).thenReturn(List.of(row));
        var unknownStagingObjectMapper = mock(PrismUnknownStagingObjectMapper.class);
        var ignoreDateRange = mock(DateRange.class);
        when(ignoreDateRange.contains(any())).thenReturn(true);
        var dispatcher = new StagingObjectDispatcher(stagingObjectHandler, stagingObjectMapper, unknownStagingObjectMapper, packetStreamMapper, ignoreDateRange);
        var stagingObjectEvent = new StagingObjectEvent("dest_bucket", "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change/2018/07/19/20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz", Instant.now(), Instant.now());
        assertThrows(EventHandler.CatchAndReleaseException.class, () -> {
            dispatcher.handleEvent(stagingObjectEvent);
        });
        verifyZeroInteractions(stagingObjectHandler);
    }
}
