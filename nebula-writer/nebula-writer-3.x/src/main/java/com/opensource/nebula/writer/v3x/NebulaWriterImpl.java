package com.opensource.nebula.writer.v3x;

import com.opensource.nebula.writer.AbstractNebulaWriter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class NebulaWriterImpl extends AbstractNebulaWriter {

    public NebulaWriterImpl(String nebulaGraphAddress) {
        super(nebulaGraphAddress);
    }

    public List<String> supportVersion() {
        return Arrays.asList("3.x");
    }

    public void executeStatement(Collection<String> statementList) {

    }
}
