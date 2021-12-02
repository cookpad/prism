package com.cookpad.prism.dao;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PacketStream {
    private long id;
    private String name;
    private boolean disabled;
    private boolean discard;
    private boolean noDispatch;
    private boolean initialized;
    private LocalDateTime createTime;
    private boolean columnInitialized;
}
