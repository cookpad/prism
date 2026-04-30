package com.cookpad.prism.batch.catalog;

import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UpsertPartitionRequest {
    @Getter
    private final String databaseName;
    @Getter
    private final String tableName;
    @Getter
    private final PartitionInput partitionInput;

    public GetPartitionRequest buildGetPartitionRequest() {
        return GetPartitionRequest.builder()
            .databaseName(this.getDatabaseName())
            .tableName(this.getTableName())
            .partitionValues(this.getPartitionInput().values())
            .build();
    }

    public CreatePartitionRequest buildCreatePartitionRequest() {
        return CreatePartitionRequest.builder()
            .databaseName(this.getDatabaseName())
            .tableName(this.getTableName())
            .partitionInput(this.getPartitionInput())
            .build();
    }

    public UpdatePartitionRequest buildUpdatePartitionRequest() {
        return UpdatePartitionRequest.builder()
            .databaseName(this.getDatabaseName())
            .tableName(this.getTableName())
            .partitionValueList(this.getPartitionInput().values())
            .partitionInput(this.getPartitionInput())
            .build();
    }
}
