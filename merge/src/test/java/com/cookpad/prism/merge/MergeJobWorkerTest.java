package com.cookpad.prism.merge;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;

import com.cookpad.prism.merge.MergeJobWorker.OpenMergeRange;
import com.cookpad.prism.record.Schema.Builder.BadSchemaError;
import com.cookpad.prism.dao.PrismMergeRange;
import com.cookpad.prism.dao.PrismSmallObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import lombok.val;

public class MergeJobWorkerTest {
    @Test
    public void testEmptyOpenMergeRange() throws BadSchemaError, IOException {
        val time = LocalDateTime.now();
        val openMergeRange = OpenMergeRange.empty(0);
        val smallObjects = new ArrayList<PrismSmallObject>();
        smallObjects.add(new PrismSmallObject(1, 1, 1, false, 30, time));
        smallObjects.add(new PrismSmallObject(2, 1, 1, false, 30, time));
        smallObjects.add(new PrismSmallObject(3, 1, 1, false, 30, time));
        smallObjects.add(new PrismSmallObject(4, 1, 1, false, 30, time));

        val mergeableObjects = openMergeRange.calculateMergeableObjectList(100, smallObjects);
        assertIterableEquals(smallObjects.subList(0, 3), mergeableObjects);
    }

    @Test
    public void testExistingOpenMergeRange() throws BadSchemaError, IOException {
        val time = LocalDateTime.now();
        val mergeRange = new PrismMergeRange(1, 1, 0, 100, 20, time, time);
        val openMergeRange = OpenMergeRange.existing(mergeRange);
        val smallObjects = new ArrayList<PrismSmallObject>();
        smallObjects.add(new PrismSmallObject(101, 1, 1, false, 30, time));
        smallObjects.add(new PrismSmallObject(102, 1, 1, false, 30, time));
        smallObjects.add(new PrismSmallObject(103, 1, 1, false, 30, time));
        smallObjects.add(new PrismSmallObject(104, 1, 1, false, 30, time));

        val mergeableObjects = openMergeRange.calculateMergeableObjectList(100, smallObjects);
        assertIterableEquals(smallObjects.subList(0, 2), mergeableObjects);
    }
}
