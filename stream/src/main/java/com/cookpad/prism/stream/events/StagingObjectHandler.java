package com.cookpad.prism.stream.events;

import com.cookpad.prism.stream.StagingObjectAttributes;
import com.cookpad.prism.dao.OneToMany;
import com.cookpad.prism.dao.PacketStream;
import com.cookpad.prism.dao.PrismStagingObject;
import com.cookpad.prism.dao.PrismTable;
import com.cookpad.prism.dao.StreamColumn;

public interface StagingObjectHandler {
    public void handleStagingObject(PrismStagingObject stagingObject, StagingObjectAttributes attrs, OneToMany<PacketStream, StreamColumn> packetStreamWithColumns, PrismTable table) throws UnknownObjectException;

    @SuppressWarnings("serial")
    public static class UnknownObjectException extends Exception {
        public UnknownObjectException(Exception cause) {
            super(cause);
        }
    }
}
