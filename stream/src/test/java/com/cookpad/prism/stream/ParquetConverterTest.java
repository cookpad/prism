package com.cookpad.prism.stream;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.objectstore.PrismObjectStore;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.objectstore.StagingObjectStore;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.record.Schema.Builder.BadSchemaError;
import com.cookpad.prism.stream.events.StagingObjectHandler.UnknownObjectException;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.PacketStream;
import com.cookpad.prism.dao.PrismMergeJobMapper;
import com.cookpad.prism.dao.PrismPartition;
import com.cookpad.prism.dao.PrismPartitionMapper;
import com.cookpad.prism.dao.PrismSmallObject;
import com.cookpad.prism.dao.PrismSmallObjectMapper;
import com.cookpad.prism.dao.PrismStagingObject;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.StreamColumn;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;

import lombok.val;

public class ParquetConverterTest {
    @Test
    public void testHandleStagingObject() throws UnknownObjectException, IOException, BadSchemaError, URISyntaxException {
        val conf = new Configuration();
        val recordWriterFactory = new RecordWriterFactory(conf);
        val stagingObjectStore = mock(StagingObjectStore.class);
        val prismSmallObjectMapper = mock(PrismSmallObjectMapper.class);
        val prismPartitionMapper = mock(PrismPartitionMapper.class);
        val prismMergeJobMapper = mock(PrismMergeJobMapper.class);
        val prismObjectStoreFactory = mock(PrismObjectStoreFactory.class);
        val schemaBuilder = new SchemaBuilder();
        val clock = Clock.fixed(Instant.ofEpochSecond(1534900000), ZoneOffset.UTC);  // 2018-08-22
        val scheduleTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(1534900000 + 43200), ZoneOffset.UTC);

        val prismTable = new PrismTable(200, null, null, "logical_example", "logical_dummy", LocalDateTime.now(), 43200);
        val objectStore = mock(PrismObjectStore.class);
        when(prismObjectStoreFactory.create(prismTable)).thenReturn(objectStore);

        val sendTime = LocalDateTime.of(2018, 8, 23, 12, 15, 0);
        val firstReceiveTime = LocalDateTime.of(2018, 8, 23, 12, 15, 30);
        val key = "2528.dwh.streaming_load.example.dummy/2018/08/23/20180823_0315_0c58b8b98-5832-4463-aae9-3514e52cc5b5.gz";
        val stagingObject = new PrismStagingObject(100, "dummy", key, sendTime, firstReceiveTime);

        try (val stagingObjectContent = this.getClass().getResourceAsStream("staging_object.gz")) {
            when(stagingObjectStore.getStagingObject(stagingObject)).thenReturn(stagingObjectContent);

            val staingObjectAttrs = new StagingObjectAttributes(
                "2528.dwh.streaming_load.example.dummy",
                "example.dummy",
                LocalDate.of(2018, 8, 23),
                "20180823_0315_0c58b8b98-5832-4463-aae9-3514e52cc5b5.gz"
            );

            val columns = new ArrayList<StreamColumn>();
            columns.add(new StreamColumn(1, "utc_event_time", "time", "timestamp", null, "+00:00", "+09:00", Instant.now(), true));
            columns.add(new StreamColumn(2, "user_id", null, "bigint", null, null, null, Instant.now(), false));
            columns.add(new StreamColumn(3, "action", null, "string", 100, null, null, Instant.now(), false));
            @SuppressWarnings("unchecked")
            OneToMany<PacketStream, StreamColumn> packetStreamWithColumns = mock(OneToMany.class);
            when(packetStreamWithColumns.getMany()).thenReturn(columns);

            val uploadStartTime = LocalDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
            val dt22 = LocalDate.of(2018, 8, 22);
            val dt23 = LocalDate.of(2018, 8, 23);
            val smallObject22 = new PrismSmallObject(322, 100, 22, true, 1000, uploadStartTime);
            val smallObject23 = new PrismSmallObject(323, 100, 23, false, 1000, uploadStartTime);

            when(prismSmallObjectMapper.findOrCreateByParams(eq(100L), eq(22L), eq(uploadStartTime), anyLong())).thenReturn(smallObject22);
            when(prismSmallObjectMapper.findOrCreateByParams(eq(100L), eq(23L), eq(uploadStartTime), anyLong())).thenReturn(smallObject23);
            when(prismPartitionMapper.createPartitionIfNotExists(200, dt22)).thenReturn(new PrismPartition(22, 200, dt22, -1, 0, null, false));
            when(prismPartitionMapper.createPartitionIfNotExists(200, dt23)).thenReturn(new PrismPartition(23, 200, dt23, -1, 0, null, false));

            val cap22 = ArgumentCaptor.forClass(File.class);
            val path22 = Files.createTempFile("prism-test-", ".parquet");
            when(objectStore.putDelayedObjectFile(eq(dt22), eq(100L), cap22.capture())).thenAnswer((inv) -> {
                Files.copy(cap22.getValue().toPath(), path22, StandardCopyOption.REPLACE_EXISTING);
                return "dummy_key22";
            });

            val cap23 = ArgumentCaptor.forClass(File.class);
            val path23 = Files.createTempFile("prism-test-", ".parquet");
            when(objectStore.putLiveObjectFile(eq(dt23), eq(100L), cap23.capture())).thenAnswer((inv) -> {
                Files.copy(cap23.getValue().toPath(), path23, StandardCopyOption.REPLACE_EXISTING);
                return "dummy_key23";
            });

            val parquetConverter = new ParquetConverter(recordWriterFactory, stagingObjectStore, prismSmallObjectMapper, prismPartitionMapper, prismMergeJobMapper, prismObjectStoreFactory, schemaBuilder, clock);
            parquetConverter.handleStagingObject(stagingObject, staingObjectAttrs, packetStreamWithColumns, prismTable);

            verify(prismSmallObjectMapper).findOrCreateByParams(eq(100L), eq(22L), eq(uploadStartTime), anyLong());
            verify(prismSmallObjectMapper).findOrCreateByParams(eq(100L), eq(23L), eq(uploadStartTime), anyLong());

            verify(objectStore).putDelayedObjectFile(eq(dt22), eq(100L), any());
            verify(objectStore).putLiveObjectFile(eq(dt23), eq(100L), any());

            verify(prismMergeJobMapper).enqueue(eq(22L), eq(scheduleTime));
            verify(prismMergeJobMapper).enqueue(eq(23L), eq(scheduleTime));
        }
    }

    @Test
    public void testIsMergeableDelay() throws Exception {
        var now = LocalDateTime.of(2020,6,20,0,0);
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,3), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,4), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,5), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,19), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,20), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,21), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,22), now));

        now = LocalDateTime.of(2020,6,20,0,59);
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,3), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,4), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,5), now));

        now = LocalDateTime.of(2020,6,20,1,0);
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,3), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,4), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,5), now));

        now = LocalDateTime.of(2020,6,20,22,0);
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,21), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,22), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,23), now));

        now = LocalDateTime.of(2020,6,20,22,59);
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,21), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,22), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,23), now));

        now = LocalDateTime.of(2020,6,20,23,0);
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,21), now));
        assertTrue(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,22), now));
        assertFalse(ParquetConverter.isMergeableDelay(LocalDate.of(2020,6,23), now));
    }

    @Test
    public void testIsAcceptable() throws Exception {
        LocalDateTime now = LocalDateTime.of(2022, 9, 9, 0, 0);

        assertTrue(ParquetConverter.isAcceptable(LocalDate.of(2022, 9, 9), now));
        assertTrue(ParquetConverter.isAcceptable(LocalDate.of(2022, 8, 19), now));
        assertFalse(ParquetConverter.isAcceptable(LocalDate.of(2022, 8, 18), now));

        assertTrue(ParquetConverter.isAcceptable(LocalDate.of(2022, 9, 12), now));
        assertFalse(ParquetConverter.isAcceptable(LocalDate.of(2022, 9, 13), now));

        now = LocalDateTime.of(2022, 9, 9, 23, 59);

        assertTrue(ParquetConverter.isAcceptable(LocalDate.of(2022, 9, 9), now));
        assertTrue(ParquetConverter.isAcceptable(LocalDate.of(2022, 8, 19), now));
        assertFalse(ParquetConverter.isAcceptable(LocalDate.of(2022, 8, 18), now));

        assertTrue(ParquetConverter.isAcceptable(LocalDate.of(2022, 9, 12), now));
        assertFalse(ParquetConverter.isAcceptable(LocalDate.of(2022, 9, 13), now));
    }
}
