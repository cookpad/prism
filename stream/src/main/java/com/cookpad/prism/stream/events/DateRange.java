package com.cookpad.prism.stream.events;

import java.time.LocalDate;
import java.util.Optional;

public class DateRange {
    private final Optional<LocalDate> startInclusive;
    private final Optional<LocalDate> endExclusive;

    public DateRange(Optional<LocalDate> startInclusive, Optional<LocalDate> endExclusive) {
        if (startInclusive.isPresent() && endExclusive.isPresent()) {
            if (startInclusive.get().compareTo(endExclusive.get()) >= 0) {
                throw new IllegalArgumentException("endExeclusive must be greater than startInclusive");
            }
        }
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    public boolean contains(LocalDate target) {
        if (!this.startInclusive.isPresent() && !this.endExclusive.isPresent()) {
            return false;
        }
        return startInclusive.map(start -> start.compareTo(target) <= 0).orElse(true)
            && endExclusive.map(end -> target.compareTo(end) < 0).orElse(true);
    }
}
