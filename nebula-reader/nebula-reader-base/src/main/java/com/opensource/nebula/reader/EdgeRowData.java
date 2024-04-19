package com.opensource.nebula.reader;

import java.util.Map;

public class EdgeRowData {

    /**
     * 起点ID
     */
    private Object srcId;
    /**
     * 目标点ID
     */
    private Object destId;

    private Long rank;

    /**
     * 属性
     */
    private Map<String, Object> properties;


    public Object getSrcId() {
        return srcId;
    }

    public void setSrcId(Object srcId) {
        this.srcId = srcId;
    }

    public Object getDestId() {
        return destId;
    }

    public void setDestId(Object destId) {
        this.destId = destId;
    }

    public Long getRank() {
        return rank;
    }

    public void setRank(Long rank) {
        this.rank = rank;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
