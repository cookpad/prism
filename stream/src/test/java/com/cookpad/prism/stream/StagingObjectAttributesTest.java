package com.cookpad.prism.stream;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StagingObjectAttributesTest {
    @Test
    void parse() throws Exception {
        var attrs = StagingObjectAttributes.parse("a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change/2018/07/19/20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz");
        assertEquals(new StagingObjectAttributes(
            "a764.dwh.streaming_load.hako_console.hako_console_autoscale_limit_change",
            "hako_console.hako_console_autoscale_limit_change",
            LocalDate.of(2018, 7, 19),
            "20180719_0429_0_364bfa27-8ed9-4218-9513-e80c97897dea.gz"
        ), attrs);
    }
}
