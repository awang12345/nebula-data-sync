package com.opensource.nebula.writer;

import java.util.Collection;
import java.util.List;

public interface NebulaWriter extends AutoCloseable {

    List<String> supportVersion();

    /**
     * 执行语句
     *
     * @param statementList
     */
    void executeStatement(Collection<String> statementList);

}
