package com.opensource.nebula.writer;

public abstract class AbstractNebulaWriter implements NebulaWriter {

    protected final String nebulaGraphAddress;

    public AbstractNebulaWriter(String nebulaGraphAddress) {
        this.nebulaGraphAddress = nebulaGraphAddress;
    }
}
