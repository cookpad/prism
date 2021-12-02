package com.cookpad.prism.objectstore;

import com.amazonaws.services.s3.AmazonS3;

import com.cookpad.prism.dao.PrismTable;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PrismObjectStoreFactory {
    final private AmazonS3 s3;
    final private PrismTableLocatorFactory objectLocator;

    public PrismObjectStore create(PrismTable table) {
        PrismTableLocator locator = this.objectLocator.build(table);
        return new PrismObjectStore(this.s3, locator);
    }
}
