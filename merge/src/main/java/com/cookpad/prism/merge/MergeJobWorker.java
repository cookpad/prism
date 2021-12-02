package com.cookpad.prism.merge;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.TempFile;
import com.cookpad.prism.SchemaBuilder.BadColumnsError;
import com.cookpad.prism.merge.downloader.DownloadedObjectSupplier;
import com.cookpad.prism.merge.downloader.MergedObjectSupplierFactory;
import com.cookpad.prism.merge.downloader.SmallObjectSupplierFactory;
import com.cookpad.prism.objectstore.MergedObjectStore;
import com.cookpad.prism.objectstore.PrismObjectStore;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.PrismMergeJob;
import com.cookpad.prism.dao.PrismMergeRange;
import com.cookpad.prism.dao.PrismMergeRangeMapper;
import com.cookpad.prism.dao.PrismPartition;
import com.cookpad.prism.dao.PrismPartitionMapper;
import com.cookpad.prism.dao.PrismSmallObject;
import com.cookpad.prism.dao.PrismSmallObjectMapper;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismTableMapper;
import com.cookpad.prism.dao.StreamColumn;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

// input: processing merge job
// output: merged objects and uploaded objects in S3 (side effect)
@Component
@RequiredArgsConstructor
public class MergeJobWorker implements MergeJobHandler {
    private final PrismMergeConf prismConf;
    private final ParallelParquetMerger parallelParquetMerger;
    private final PrismMergeRangeMapper mergeRangeMapper;
    private final PrismSmallObjectMapper smallObjectMapper;
    private final PrismPartitionMapper partitionMapper;
    private final PrismTableMapper tableMapper;
    private final PrismObjectStoreFactory objectStoreFactory;
    private final Clock clock;

    @Override
    public JobStatus handleJob(PrismMergeJob job) throws IOException, BadColumnsError {
        JobStatus status = JobStatus.FINISHED;

        final int BATCH_SIZE = this.prismConf.getMergeBatchSize();
        final long MAX_SIZE = this.prismConf.getMergedObjectSize();

        final PrismPartition partition = this.partitionMapper.find(job.getPartitionId());
        final OneToMany<PrismTable, StreamColumn> tableWithColumns = this.tableMapper.findWithColumns(partition.getTableId());
        final PrismTable table = tableWithColumns.getOne();
        final Schema schema = new SchemaBuilder().build(tableWithColumns.getOne(), tableWithColumns.getMany());

        final PrismObjectStore prismObjectStore = this.objectStoreFactory.create(table);

        final PrismMergeRange mergeRange = this.mergeRangeMapper.findOpenRange(job.getPartitionId());
        OpenMergeRange openMergeRange;
        if (mergeRange == null) {
            openMergeRange = OpenMergeRange.first();
        } else {
            openMergeRange = OpenMergeRange.existing(mergeRange);
        }

        final List<PrismSmallObject> newSmallObjects = this.smallObjectMapper.findNewObjects(partition.getId(), openMergeRange.getMaxMergedId(), BATCH_SIZE);
        if (newSmallObjects.size() == BATCH_SIZE) {
            // the job is potentially incompleted
            // if the number of returning objects reaches the limit
            status = JobStatus.CONTINUING;
        }
        if (newSmallObjects.size() == 0) {
            return JobStatus.FINISHED;
        }

        MergePlanner planner = new MergePlanner(MAX_SIZE, partition);
        MergePlan plan = planner.makePlan(openMergeRange, newSmallObjects);
        if (plan.isFull()) {
            status = JobStatus.CONTINUING;
        }
        MergePlanExecutor planExecutor = MergePlanExecutor.of(
            mergeRangeMapper,
            parallelParquetMerger,
            prismObjectStore,
            clock
        );
        planExecutor.execute(schema, plan);
        this.partitionMapper.updateDesiredManifestVersion(job.getPartitionId(), plan.getUpperBound());

        return status;
    }

    @Override
    public void shutdown() {
        parallelParquetMerger.shutdown();
    }

    @RequiredArgsConstructor
    public static class OpenMergeRange {
        @Getter
        private final long maxMergedId;
        @Getter
        private final Optional<PrismMergeRange> existingMergeRange;

        public static OpenMergeRange first() {
            return empty(0);
        }

        public static OpenMergeRange empty(long prevUpperBound) {
            return new OpenMergeRange(prevUpperBound, Optional.empty());
        }

        public static OpenMergeRange existing(PrismMergeRange mergeRange) {
            return new OpenMergeRange(mergeRange.getUpperBound(), Optional.of(mergeRange));
        }

        public long getCurrentSize() {
            return this.existingMergeRange.map(PrismMergeRange::getContentLength).orElse(0L);
        }

        public long getLowerBound() {
            return this.getExistingMergeRange().map(PrismMergeRange::getLowerBound).orElse(this.getMaxMergedId());
        }

        public List<PrismSmallObject> calculateMergeableObjectList(long maxSize, List<PrismSmallObject> newSmallObjects) {
            long restSize = maxSize - this.getCurrentSize();
            int i;
            for (i = 0; i < newSmallObjects.size(); i ++) {
                long size = newSmallObjects.get(i).getContentLength();
                if (size > restSize) {
                    break;
                }
                restSize -= size;
            }
            return newSmallObjects.subList(0, i);
        }
    }

    @RequiredArgsConstructor
    public static class MergePlanner {
        private final long mergedObjectMaxSize;
        private final PrismPartition partition;

        public Optional<MergePlan> tryToMakePlan(OpenMergeRange openMergeRange, List<PrismSmallObject> newSmallObjects) {
            List<PrismSmallObject> mergeableObjects = openMergeRange.calculateMergeableObjectList(this.mergedObjectMaxSize, newSmallObjects);
            if (mergeableObjects.size() == 0) {
                return Optional.empty();
            }
            boolean hasLeftOff = mergeableObjects.size() < newSmallObjects.size();
            return Optional.of(new MergePlan(
                partition,
                openMergeRange.getLowerBound(),
                hasLeftOff,
                mergeableObjects,
                openMergeRange.getExistingMergeRange()
            ));
        }

        public MergePlan makePlan(OpenMergeRange openMergeRange, List<PrismSmallObject> newSmallObjects) {
            Optional<MergePlan> maybePlan = this.tryToMakePlan(openMergeRange, newSmallObjects);
            MergePlan plan = maybePlan.orElseGet(() -> {
                OpenMergeRange emptyOpenMergeRange = OpenMergeRange.empty(openMergeRange.getMaxMergedId());
                return this.tryToMakePlan(emptyOpenMergeRange, newSmallObjects).get();
            });
            return plan;
        }
    }

    @RequiredArgsConstructor
    @Getter
    public static class MergePlan {
        private final PrismPartition partition;
        private final long lowerBound;
        private final boolean isFull;
        private final List<PrismSmallObject> newSmallObjects;
        private final Optional<PrismMergeRange> oldMergeRange;

        public long getUpperBound() {
            return this.newSmallObjects.get(this.newSmallObjects.size() - 1).getId();
        }
    }

    @RequiredArgsConstructor
    public static class MergePlanExecutor {
        private final PrismMergeRangeMapper mergeRangeMapper;
        private final ParallelParquetMerger parallelParquetMerger;
        private final MergedObjectStore mergedObjectStore;
        private final SmallObjectSupplierFactory smallObjectSupplierFactory;
        private final MergedObjectSupplierFactory mergedObjectSupplierFactory;
        private final Clock clock;

        public LocalDateTime now() {
            return LocalDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
        }

        public static MergePlanExecutor of(PrismMergeRangeMapper mergeRangeMapper, ParallelParquetMerger parallelParquetMerger, PrismObjectStore prismObjectStore, Clock clock) {
            SmallObjectSupplierFactory smallObjectSupplierFactory = new SmallObjectSupplierFactory(prismObjectStore);
            MergedObjectSupplierFactory mergedObjectSupplierFactory = new MergedObjectSupplierFactory(prismObjectStore);
            return new MergePlanExecutor(mergeRangeMapper, parallelParquetMerger, prismObjectStore, smallObjectSupplierFactory, mergedObjectSupplierFactory, clock);
        }

        public void execute(Schema schema, MergePlan plan) throws IOException {
            List<DownloadedObjectSupplier> smallObjectSuppliers = this.smallObjectSupplierFactory.createMultipleSuppliers(plan.getNewSmallObjects(), plan.getPartition());
            ParallelParquetMerger.Node rootOfSmallObjects = this.parallelParquetMerger.buildTree(smallObjectSuppliers);
            ParallelParquetMerger.Node root = plan.getOldMergeRange().map((mergeRange) -> {
                DownloadedObjectSupplier supplier = this.mergedObjectSupplierFactory.createSupplier(mergeRange, plan.getPartition());
                return this.parallelParquetMerger.toMergeNode(rootOfSmallObjects, this.parallelParquetMerger.toNode(supplier));
            }).orElse(rootOfSmallObjects);

            try(final TempFile output = this.parallelParquetMerger.mergeTree(schema, root)) {
                File outputFile = output.getPath().toFile();
                long contentLength = outputFile.length();
                mergedObjectStore.putMergedObjectFile(
                    plan.getPartition().getPartitionDate(),
                    plan.getLowerBound(),
                    plan.getUpperBound(),
                    outputFile
                );

                this.mergeRangeMapper.upsertRange(plan.getPartition().getId(), plan.getLowerBound(), plan.getUpperBound(), contentLength, now());
            }
        }
    }
}
