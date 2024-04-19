package com.opensource.nebula.reader;

import java.util.Map;

public class VertexRowData {

    private Object vid;

    /**
     * 属性
     */
    private Map<String, Object> properties;

    public Object getVid() {
        return vid;
    }

    public void setVid(Object vid) {
        this.vid = vid;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
