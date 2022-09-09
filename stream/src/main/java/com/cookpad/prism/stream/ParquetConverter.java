package com.cookpad.prism.stream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.time.LocalDate;

import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.TempFile;
import com.cookpad.prism.SchemaBuilder.BadColumnsError;
import com.cookpad.prism.jsonl.JsonlReader;
import com.cookpad.prism.jsonl.JsonlRecordReader;
import com.cookpad.prism.objectstore.PrismObjectStore;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.objectstore.StagingObjectStore;
import com.cookpad.prism.record.RecordTimestampComparator;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.stream.events.StagingObjectHandler;
import com.cookpad.prism.record.partitioned.PartitionedRecordWriter;
import com.cookpad.prism.record.partitioned.SortedPartitionedWriter;
import com.cookpad.prism.record.partitioned.DateAttachedRecord;
import com.cookpad.prism.record.partitioned.PartitionCollector;
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
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
public class ParquetConverter implements StagingObjectHandler {
    private final RecordWriterFactory recordWriterFactory;
    private final StagingObjectStore stagingObjectStore;
    private final PrismSmallObjectMapper smallObjectMapper;
    private final PrismPartitionMapper partitionMapper;
    private final PrismMergeJobMapper mergeJobMapper;
    private final PrismObjectStoreFactory prismObjectStoreFactory;
    private final SchemaBuilder schemaBuilder;
    private final Clock clock;

    // FIXME: fixed lower bound: 2018-01-01 (inclusive)
    static final LocalDate PARTITION_DATE_LOWER_BOUND = LocalDate.of(2018, 1, 1);

    @Override
    public void handleStagingObject(@NonNull PrismStagingObject stagingObject, @NonNull StagingObjectAttributes attrs, @NonNull OneToMany<PacketStream, StreamColumn> packetStreamWithColumns, @NonNull PrismTable table) throws UnknownObjectException {
        Schema schema;
        try {
            schema = schemaBuilder.build(table, packetStreamWithColumns.getMany());
        } catch (BadColumnsError e) {
            throw new UnknownObjectException(e);
        }
        TempFile.Factory tempFileFactory = new TempFile.Factory("prism-stream-", ".parquet");
        try (PartitionCollector partitionCollector = new PartitionCollector()) {
            ArrayList<DateAttachedRecord> recordsBuffer = new ArrayList<>(80000);
            try (
                InputStream gzipped = stagingObjectStore.getStagingObject(stagingObject);
                GZIPInputStream unzipped = new GZIPInputStream(gzipped);
                InputStreamReader isr = new InputStreamReader(unzipped, StandardCharsets.UTF_8);
                LineNumberReader lnr = new LineNumberReader(isr);
                JsonlReader reader = new JsonlReader(lnr);
                JsonlRecordReader recordReader = new JsonlRecordReader(schema, reader);
            ) {
                DateAttachedRecord record;
                long discarded = 0;
                while ((record = recordReader.read()) != null) {
                    var dt = record.getPartitionDate();
                    if (dt.isAfter(PARTITION_DATE_LOWER_BOUND) || dt.isEqual(PARTITION_DATE_LOWER_BOUND)) {
                        recordsBuffer.add(record);
                    }
                    else {
                        discarded++;
                    }
                }
                if (discarded > 0) {
                    log.info("{}: too old records discarded: count={}", stagingObject.getObjectUri(), discarded);
                }
            }

            recordsBuffer.sort(new RecordTimestampComparator());

            try (
                PartitionedRecordWriter writer = new SortedPartitionedWriter(this.recordWriterFactory, tempFileFactory, partitionCollector, schema);
            ) {
                for (DateAttachedRecord record : recordsBuffer) {
                    writer.write(record);
                }
            }

            PrismObjectStore prismObjectStore = this.prismObjectStoreFactory.create(table);
            TreeMap<LocalDate, Path> partitions = partitionCollector.collect();
            // This is normal situation, do not warn it
            //if (partitions.size() == 0) {
            //    log.error("No records in staging object: {}", stagingObject);
            //}
            for (Entry<LocalDate, Path> entry: partitions.entrySet()) {
                LocalDate dt = entry.getKey();
                File file = entry.getValue().toFile();
                LocalDateTime now = LocalDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
                PrismPartition partition = this.partitionMapper.createPartitionIfNotExists(table.getId(), dt);
                long contentLength = file.length();
                PrismSmallObject smallObject = this.smallObjectMapper.findOrCreateByParams(stagingObject.getId(), partition.getId(), now, contentLength);
                if (smallObject.isDelayed()) {
                    prismObjectStore.putDelayedObjectFile(dt, stagingObject.getId(), file);
                } else {
                    prismObjectStore.putLiveObjectFile(dt, stagingObject.getId(), file);
                }
                if (isMergeableDelay(dt, now)) {
                    LocalDateTime scheduleTime = table.scheduleTime(now);
                    this.mergeJobMapper.enqueue(partition.getId(), scheduleTime);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Encountered an error in converting JSONL to Parquet", ex);
        }
    }

    static final long MERGEABLE_DELAY_DAYS = 14;

    static boolean isMergeableDelay(LocalDate partitionDate, LocalDateTime now) {
        // +1 day as max timezone offset
        // +1 hour for spare (processing delay, S3 event delay, etc)
        LocalDate minMergeableDate = now.minusDays(MERGEABLE_DELAY_DAYS + 1).minusHours(1).toLocalDate();
        LocalDate maxMergeableDate = now.plusDays(1).plusHours(1).toLocalDate();
        return (partitionDate.isEqual(minMergeableDate) || partitionDate.isAfter(minMergeableDate))
            && (partitionDate.isEqual(maxMergeableDate) || partitionDate.isBefore(maxMergeableDate));
    }
}
