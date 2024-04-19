package com.opensource.nebula.reader;

public abstract class AbstractNebulaReader implements NebulaReader{

    protected final String nebulaMetaAddress;

    public AbstractNebulaReader(String nebulaMetaAddress) {
        this.nebulaMetaAddress = nebulaMetaAddress;
    }
}
