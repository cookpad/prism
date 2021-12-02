package com.cookpad.prism.dao;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrismTable {
    private int id;
    private String physicalSchemaName;
    private String physicalTableName;
    private String logicalSchemaName;
    private String logicalTableName;
    private LocalDateTime createTime;
    private int mergeInterval;

    public String getPhysicalSchemaName() {
        if (this.physicalSchemaName != null) {
            return this.physicalSchemaName;
        }
        return this.logicalSchemaName;
    }

    public String getPhysicalTableName() {
        if (this.physicalTableName != null) {
            return this.physicalTableName;
        }
        return this.logicalTableName;
    }

    public String getPhysicalFullName() {
        return String.format("%s.%s", this.getPhysicalSchemaName(), this.getPhysicalTableName());
    }

    public String getLogicalFullName() {
        return String.format("%s.%s", this.getLogicalSchemaName(), this.getLogicalTableName());
    }

    public LocalDateTime scheduleTime(LocalDateTime now) {
        return now.plusSeconds(this.mergeInterval);
    }
}
