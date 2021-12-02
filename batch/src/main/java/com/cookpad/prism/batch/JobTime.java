package com.cookpad.prism.batch;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class JobTime {
    final private Clock clock;
    private Instant time;

    @NonNull
    public Instant getTime() {
        if (this.time == null) {
            this.time = this.clock.instant();
        }
        return this.time;
    }

    public LocalDateTime getTimeInUTC() {
        return LocalDateTime.ofInstant(this.getTime(), ZoneOffset.UTC);
    }
}
