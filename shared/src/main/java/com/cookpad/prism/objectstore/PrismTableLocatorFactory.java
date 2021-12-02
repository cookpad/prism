package com.cookpad.prism.objectstore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.cookpad.prism.dao.PrismTable;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PrismTableLocatorFactory {
    @Getter
    final private String bucketName;
    final private String globalPrefix;

    private String getHashPrefixedTableName(String schemaName, String tableName) {
        String fullName = String.format("%s.%s", schemaName, tableName);
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hash = md5.digest(fullName.getBytes(StandardCharsets.UTF_8));
        String prefixedTableName = String.format("%02x%02x.%s", hash[0], hash[1], fullName);
        return prefixedTableName;
    }

    private PrismTableLocator buildWithPhysicalNames(String schemaName, String tableName) {
        String hashPrefixedTableName = this.getHashPrefixedTableName(schemaName, tableName);
        String tablePrefix = String.format("%s%s/", this.globalPrefix, hashPrefixedTableName);
        return new PrismTableLocator(this.bucketName, tablePrefix);
    }

    public PrismTableLocator build(PrismTable table) {
        return this.buildWithPhysicalNames(table.getPhysicalSchemaName(), table.getPhysicalTableName());
    }
}
