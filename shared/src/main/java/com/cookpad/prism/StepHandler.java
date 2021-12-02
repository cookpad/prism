package com.cookpad.prism;

public interface StepHandler {
    /**
     * return false if it is done successfully
     * return true if it continues
     */
    public boolean handleStep();

    public void shutdown();
}
