package com.cookpad.prism.stream.events;

public interface EventHandler {
    void handleEvent(StagingObjectEvent event) throws CatchAndReleaseException;

    @SuppressWarnings("serial")
    public static class CatchAndReleaseException extends Exception {}
}
