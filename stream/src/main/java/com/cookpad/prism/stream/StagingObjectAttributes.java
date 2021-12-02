package com.cookpad.prism.stream;

import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
@Getter
@EqualsAndHashCode
public class StagingObjectAttributes {
    private static Pattern PATTERN = Pattern.compile("(\\w{4}\\.(?:\\w+\\.){0,2}(\\w+\\.\\w+))/(\\d{4})/(\\d{2})/(\\d{2})/(.*\\.gz)");

    private final String streamPrefix;
    private final String streamName;
    private final LocalDate date;
    private final String objectName;

    public static StagingObjectAttributes parse(@NonNull String key) throws NotAnStagingObjectException {
        Matcher m = PATTERN.matcher(key);
        if (!m.matches()) {
            throw new NotAnStagingObjectException();
        }
        String streamPrefix = m.group(1);
        String streamName = m.group(2);
        String yyyy = m.group(3);
        String mm = m.group(4);
        String dd = m.group(5);
        int year = Integer.parseInt(yyyy, 10);
        int month = Integer.parseInt(mm, 10);
        int dayOfMonth = Integer.parseInt(dd, 10);
        LocalDate date = LocalDate.of(year, month, dayOfMonth);
        String objectName = m.group(6);
        return new StagingObjectAttributes(streamPrefix, streamName, date, objectName);
    }

    @SuppressWarnings("serial")
    public static class NotAnStagingObjectException extends Exception {}
}
