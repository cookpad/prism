package com.cookpad.prism.merge;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.SchemaBuilder.BadColumnsError;
import com.cookpad.prism.merge.MergeJobWorker.MergePlan;
import com.cookpad.prism.merge.MergeJobWorker.MergePlanExecutor;
import com.cookpad.prism.objectstore.PrismObjectStore;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.PrismMergeRange;
import com.cookpad.prism.dao.PrismMergeRangeMapper;
import com.cookpad.prism.dao.PrismPartition;
import com.cookpad.prism.dao.PrismPartitionMapper;
import com.cookpad.prism.dao.PrismSmallObject;
import com.cookpad.prism.dao.PrismSmallObjectMapper;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismTableMapper;
import com.cookpad.prism.dao.StreamColumn;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@Lazy
@RequiredArgsConstructor
public class RebuildCmd {
    private final ParallelParquetMerger parallelParquetMerger;
    private final PrismMergeRangeMapper mergeRangeMapper;
    private final PrismSmallObjectMapper smallObjectMapper;
    private final PrismPartitionMapper partitionMapper;
    private final PrismTableMapper tableMapper;
    private final PrismObjectStoreFactory objectStoreFactory;
    private final Clock clock;

    public void run(int tableId, List<LocalDate> partitionDates) throws BadColumnsError, IOException {
        OneToMany<PrismTable, StreamColumn> tableWithColumns = tableMapper.findWithColumns(tableId);
        PrismTable table = tableWithColumns.getOne();
        Schema schema = new SchemaBuilder().build(tableWithColumns.getOne(), tableWithColumns.getMany());
        PrismObjectStore prismObjectStore = this.objectStoreFactory.create(table);
        MergePlanExecutor planExecutor = MergePlanExecutor.of(
            mergeRangeMapper,
            parallelParquetMerger,
            prismObjectStore,
            clock
        );
        for (LocalDate date : partitionDates) {
            PrismPartition partition = this.partitionMapper.findByTableIdAndDate(tableId, date);
            if (partition == null) {
                continue;
            }
            List<PrismMergeRange> mergeRanges = this.mergeRangeMapper.findAllInPartition(partition.getId());
            for (PrismMergeRange mergeRange : mergeRanges) {
                List<PrismSmallObject> smallObjects = this.smallObjectMapper.findAllObjectsInRange(partition.getId(), mergeRange.getLowerBound(), mergeRange.getUpperBound());
                MergePlan plan = new MergePlan(partition, mergeRange.getLowerBound(), false, smallObjects, Optional.empty());
                planExecutor.execute(schema, plan);
            }
            this.partitionMapper.updateCurrentManifestVersion(partition.getId(), -1);
        }
    }
}
