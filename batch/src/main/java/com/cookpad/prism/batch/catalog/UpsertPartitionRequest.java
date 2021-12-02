package com.cookpad.prism.batch.catalog;

import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;

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
        return new GetPartitionRequest()
            .withDatabaseName(this.getDatabaseName())
            .withTableName(this.getTableName())
            .withPartitionValues(this.getPartitionInput().getValues())
        ;
    }

    public CreatePartitionRequest buildCreatePartitionRequest() {
        return new CreatePartitionRequest()
            .withDatabaseName(this.getDatabaseName())
            .withTableName(this.getTableName())
            .withPartitionInput(this.getPartitionInput())
        ;
    }

    public UpdatePartitionRequest buildUpdatePartitionRequest() {
        return new UpdatePartitionRequest()
            .withDatabaseName(this.getDatabaseName())
            .withTableName(this.getTableName())
            .withPartitionValueList(this.getPartitionInput().getValues())
            .withPartitionInput(this.getPartitionInput())
        ;
    }
}
