package com.opensource.nebula.reader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface NebulaReader extends AutoCloseable {

    /**
     * 执行哪些nebula版本
     *
     * @return
     */
    List<String> supportVersions();

    /**
     * 获取所有的分片
     *
     * @return
     */
    List<Integer> getAllPartList(String space);

    /**
     * 获取所有的边
     *
     * @param space
     * @return
     */
    List<EdgeDomain> getAllEdgeList(String space);

    /**
     * 获取所有的tag
     *
     * @param space
     * @return
     */
    List<TagDomain> getAllTagList(String space);

    /**
     * 扫描边
     *
     * @param space
     * @param part
     * @param returnEdgeColumnListMap 返回的边和属性列表
     * @param scanLimit
     * @param startTime
     * @param endTime
     * @return insert语句列表
     */
    Iterator<List<EdgeRowData>> scanEdge(String space, int part, Map<String, List<String>> returnEdgeColumnListMap, int scanLimit, long startTime, long endTime) throws IOException;

    /**
     * 扫描点
     *
     * @param space
     * @param part
     * @param returnTagColumnListMap 返回的点和属性列表
     * @param scanLimit
     * @param startTime
     * @param endTime
     * @return insert语句列表
     */
    Iterator<List<VertexRowData>> scanVertex(String space, int part, Map<String, List<String>> returnTagColumnListMap, int scanLimit, long startTime, long endTime) throws IOException;

}
