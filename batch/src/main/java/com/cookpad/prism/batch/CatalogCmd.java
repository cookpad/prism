package com.cookpad.prism.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cookpad.prism.SchemaBuilder;
import com.cookpad.prism.SchemaBuilder.BadColumnsError;
import com.cookpad.prism.batch.catalog.Catalog;
import com.cookpad.prism.batch.catalog.CatalogTable;
import com.cookpad.prism.batch.catalog.UpsertPartitionRequest;
import com.cookpad.prism.objectstore.MergedObjectStore;
import com.cookpad.prism.objectstore.PartitionManifest;
import com.cookpad.prism.objectstore.PrismObjectStoreFactory;
import com.cookpad.prism.objectstore.PrismTableLocator;
import com.cookpad.prism.objectstore.PrismTableLocatorFactory;
import com.cookpad.prism.record.Schema;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.PrismMergeRange;
import com.cookpad.prism.dao.PrismMergeRangeMapper;
import com.cookpad.prism.dao.PrismPartition;
import com.cookpad.prism.dao.PrismPartitionMapper;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.PrismTableMapper;
import com.cookpad.prism.dao.StreamColumn;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Component
@Slf4j
@Lazy
public class CatalogCmd {
    private final PrismTableMapper tableMapper;
    private final PrismPartitionMapper partitionMapper;
    private final PrismMergeRangeMapper mergeRangeMapper;
    private final Catalog catalog;
    private final CatalogTable.Factory catalogTableFactory;
    private final PrismTableLocatorFactory locatorFactory;
    private final PrismObjectStoreFactory objectStoreFactory;
    private final JobTime jobTime;

    private void updateCatalog() {
        List<OneToMany<PrismTable, StreamColumn>> tablesWithPartitionSourceColumn = tableMapper.getAllWithColumns();

        Map<Integer, CatalogTable> catalogTables = new HashMap<>();
        Map<Integer, PrismTableLocator> tableLocators = new HashMap<>();
        Map<Integer, MergedObjectStore> mergedObjectStores = new HashMap<>();
        for (OneToMany<PrismTable, StreamColumn> tableWithColumn: tablesWithPartitionSourceColumn) {
            PrismTable table = tableWithColumn.getOne();
            log.debug("table: {}", table.getLogicalFullName());
            List<StreamColumn> columns = tableWithColumn.getMany();
            Schema schema;
            try {
                schema = new SchemaBuilder().build(table, columns);
            } catch (BadColumnsError ex) {
                log.error("Schema is corrupted: {}", table, ex);
                continue;
            }
            PrismTableLocator tableLocator = this.locatorFactory.build(table);
            CatalogTable catalogTable = this.catalogTableFactory.build(schema, tableLocator);
            catalogTables.put(table.getId(), catalogTable);
            tableLocators.put(table.getId(), tableLocator);
            mergedObjectStores.put(table.getId(), this.objectStoreFactory.create(table));
        }
        this.catalog.upsertTables(new ArrayList<>(catalogTables.values()));

        List<PrismPartition> newPartitions = this.partitionMapper.getNewPartitions();
        for (PrismPartition partition : newPartitions) {
            if (!catalogTables.containsKey(partition.getTableId())) {
                continue;
            }

            CatalogTable catalogTable = catalogTables.get(partition.getTableId());
            UpsertPartitionRequest upsertPartitionRequest = catalogTable.buildUpsertPartitionRequest(
                partition.getPartitionDate(),
                partition.isSwitched(),
                partition.getDesiredManifestVersion()
            );
            this.catalog.upsertPartition(upsertPartitionRequest);
            this.partitionMapper.updateCurrentManifestVersion(partition.getId(), 0);
        }
        List<PrismPartition> partitionsToUpdate = this.partitionMapper.getSwitchedPartitionsToUpdate();
        for (PrismPartition partition : partitionsToUpdate) {
            if (!catalogTables.containsKey(partition.getTableId())) {
                continue;
            }

            // [ generate & put manifest ]
            List<PrismMergeRange> mergeRanges = this.mergeRangeMapper.findAllInPartition(partition.getId());
            PrismMergeRange lastMergeRange = mergeRanges.get(mergeRanges.size() - 1);
            // skip if it's updated while this job is running
            if (lastMergeRange.getUpperBound() != partition.getDesiredManifestVersion()) {
                continue;
            }

            PrismTableLocator tableLocator = tableLocators.get(partition.getTableId());
            MergedObjectStore mergedObjectStore = mergedObjectStores.get(partition.getTableId());

            PartitionManifest manifest = new ManifestGenerator().
                generate(tableLocator, partition.getPartitionDate(), mergeRanges);

            mergedObjectStore.putMergedPartitionManifest(
                partition.getPartitionDate(),
                partition.getDesiredManifestVersion(),
                manifest.toJSON()
            );
            // [ / generate & put manifest ]

            CatalogTable catalogTable = catalogTables.get(partition.getTableId());
            UpsertPartitionRequest upsertPartitionRequest = catalogTable.buildUpsertPartitionRequest(
                partition.getPartitionDate(),
                partition.isSwitched(),
                partition.getDesiredManifestVersion()
            );
            this.catalog.upsertPartition(upsertPartitionRequest);
            this.partitionMapper.updateCurrentManifestVersion(partition.getId(), partition.getDesiredManifestVersion());
        }
    }

    public void run() {
        this.partitionMapper.switchPartitions();
        this.partitionMapper.closePartitions(this.jobTime.getTimeInUTC());
        this.updateCatalog();
    }
}
