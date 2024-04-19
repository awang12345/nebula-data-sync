package com.opensource.nebula.reader.v1_x;

import com.facebook.thrift.TException;
import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import com.opensource.nebula.reader.*;
import com.vesoft.nebula.client.meta.MetaClientImpl;
import com.vesoft.nebula.client.storage.StorageClientImpl;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.meta.TagItem;
import com.vesoft.nebula.storage.ScanEdge;
import com.vesoft.nebula.storage.ScanEdgeResponse;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class NebulaReaderImpl extends AbstractNebulaReader {

    private MetaClientImpl metaClient;

    private StorageClientImpl storageClient;

    public NebulaReaderImpl(String nebulaMetaAddress) throws TException {
        super(nebulaMetaAddress);
        Iterable<String> addressIt = Splitter.on(',').trimResults().split(nebulaMetaAddress);
        List<HostAndPort> hostAndPortList = new ArrayList<>();
        addressIt.forEach(address -> hostAndPortList.add(HostAndPort.fromString(address)));
        metaClient = new MetaClientImpl(hostAndPortList);
        metaClient.connect();
        storageClient = new StorageClientImpl(metaClient);
    }

    public List<String> supportVersions() {
        return Arrays.asList("1.x");
    }

    public List<Integer> getAllPartList(String space) {
        Set<Integer> parts = metaClient.getPartsAlloc(space).keySet();
        return new ArrayList<>(parts);
    }

    public List<EdgeDomain> getAllEdgeList(String space) {
        List<EdgeItem> edges = metaClient.getEdges(space);
        if (edges == null || edges.isEmpty()) {
            return Collections.emptyList();
        }
        return edges.stream().map(edgeItem -> {
            EdgeDomain edgeDomain = new EdgeDomain();
            edgeDomain.setEdgeName(edgeItem.edge_name);
            edgeDomain.setEdgeType(edgeItem.edge_type);
            return edgeDomain;
        }).collect(Collectors.toList());
    }

    public List<TagDomain> getAllTagList(String space) {
        List<TagItem> tags = metaClient.getTags(space);
        if (tags == null || tags.isEmpty()) {
            return Collections.emptyList();
        }
        return tags.stream().map(edgeItem -> {
            TagDomain tagDomain = new TagDomain();
            tagDomain.setTagId(edgeItem.tag_id);
            tagDomain.setTagName(edgeItem.tag_name);
            return tagDomain;
        }).collect(Collectors.toList());
    }

    public Iterator<List<EdgeRowData>> scanEdge(String space, int part, Map<String, List<String>> returnEdgeColumnListMap, int scanLimit, long startTime, long endTime) throws IOException {
        Iterator<ScanEdgeResponse> iterator = storageClient.scanEdge(space, part, returnEdgeColumnListMap, true, scanLimit, startTime, endTime);
        return new Iterator<List<EdgeRowData>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public List<EdgeRowData> next() {
                ScanEdgeResponse response = iterator.next();
//                response.getResult()
                List<ScanEdge> edge_data = response.getEdge_data();
                for (ScanEdge edge_datum : edge_data) {
//                    edge_datum.get
                }
                return null;
            }
        };
    }

    public Iterator<List<VertexRowData>> scanVertex(String space, int part, Map<String, List<String>> returnTagColumnListMap, int scanLimit, long startTime, long endTime) {
        return null;
    }

    @Override
    public void close() throws Exception {
        metaClient.close();
    }
}
