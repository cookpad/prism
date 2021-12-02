package com.cookpad.prism.stream.events;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.Optional;

public class DateRangeTest {
    final static LocalDate earliest = LocalDate.of(2018, 8, 8);
    final static LocalDate earlier = LocalDate.of(2018, 8, 9);
    final static LocalDate middle = LocalDate.of(2018, 8, 10);
    final static LocalDate later = LocalDate.of(2018, 8, 11);
    final static LocalDate latest = LocalDate.of(2018, 8, 12);

    @Test
    public void testStartOnly() {
        var range = new DateRange(Optional.of(middle), Optional.empty());
        assertFalse(range.contains(earliest));
        assertFalse(range.contains(earlier));
        assertTrue(range.contains(middle)); // <- start
        assertTrue(range.contains(later));
        assertTrue(range.contains(latest));
    }

    @Test
    public void testEndOnly() {
        var range = new DateRange(Optional.empty(), Optional.of(middle));
        assertTrue(range.contains(earliest));
        assertTrue(range.contains(earlier));
        assertFalse(range.contains(middle)); // <- end
        assertFalse(range.contains(later));
        assertFalse(range.contains(latest));
    }

    @Test
    public void testBothStartAndEnd() {
        var range = new DateRange(Optional.of(earlier), Optional.of(later));
        assertFalse(range.contains(earliest));
        assertTrue(range.contains(earlier)); // <- start
        assertTrue(range.contains(middle));
        assertFalse(range.contains(later)); // <- end
        assertFalse(range.contains(latest));
    }

    @Test
    public void testNothing() {
        var range = new DateRange(Optional.empty(), Optional.empty());
        assertFalse(range.contains(earliest));
        assertFalse(range.contains(earlier));
        assertFalse(range.contains(middle));
        assertFalse(range.contains(later));
        assertFalse(range.contains(latest));
    }
}
