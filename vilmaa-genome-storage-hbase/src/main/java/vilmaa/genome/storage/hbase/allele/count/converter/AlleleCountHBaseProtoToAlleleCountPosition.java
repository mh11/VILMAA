package vilmaa.genome.storage.hbase.allele.count.converter;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.models.protobuf.AlleleMap;
import vilmaa.genome.storage.hbase.allele.models.protobuf.AlternateCount;
import vilmaa.genome.storage.hbase.allele.models.protobuf.ReferenceCountHBaseProto;
import vilmaa.genome.storage.hbase.allele.models.protobuf.SampleList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleCountHBaseProtoToAlleleCountPosition {

    public AlleleCountPosition referenceFromBytes(byte[] arr) throws IOException {
        AlleleCountPosition position = new AlleleCountPosition();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(arr);
        ReferenceCountHBaseProto referenceProto = ReferenceCountHBaseProto.parseDelimitedFrom(inputStream);
        while (referenceProto != null) {
            update(position, referenceProto);
            referenceProto = ReferenceCountHBaseProto.parseDelimitedFrom(inputStream);
        }
        return position;
    }


    public AlleleCountPosition variantFromBytes(byte[] bytes) throws IOException {
        AlleleCountPosition position = new AlleleCountPosition();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        AlleleMap alternateCount = AlleleMap.parseDelimitedFrom(inputStream);
        while (alternateCount != null) {
            update(position, alternateCount);
            alternateCount = AlleleMap.parseDelimitedFrom(inputStream);
        }
        return position;
    }

    public AlleleCountPosition convert(ReferenceCountHBaseProto proto) {
        AlleleCountPosition position = new AlleleCountPosition();
        update(position, proto);
        return position;
    }

    public void update(AlleleCountPosition position, AlleleMap proto) {
        if (null == proto) {
            return;
        }
        merge(position.getAlternate(), getAllels(proto));
    }

    public void update(AlleleCountPosition position, ReferenceCountHBaseProto proto) {
        if (null == proto) {
            return;
        }
        position.getNotPass().addAll(getCollection(proto.getNotPass()));
        merge(position.getReference(), getAllels(proto.getReference()));
        mergeAlt(position.getAltMap(), getAltMap(proto.getAlternates()));
    }

    private void mergeAlt(Map<String, Map<Integer, List<Integer>>> to, Map<String, Map<Integer, List<Integer>>> from) {
        if (to.isEmpty()) {
            to.putAll(from);
            return;
        }
        from.forEach((k, v) -> {
            Map<Integer, List<Integer>> map = to.get(k);
            if (null == map) {
                to.put(k, v);
            } else if (map instanceof HashMap){
                merge(map, v);
            } else {
                HashMap<Integer, List<Integer>> newMap = new HashMap<>();
                merge(newMap, map);
                merge(newMap, v);
                to.put(k, newMap);
            }
        });
    }

    private void merge(Map<Integer, List<Integer>> to, Map<Integer, List<Integer>> from) {
        if (to.isEmpty()) {
            to.putAll(from);
            return;
        }
        from.forEach((k, v) -> {
            List<Integer> lst = to.get(k);
            if (null == lst) {
                to.put(k, v);
            } else if (lst instanceof ArrayList){
                lst.addAll(v);
            } else { // Unmodifiable Set
                List<Integer> newLst = new ArrayList<>(lst.size() + v.size());
                newLst.addAll(lst);
                newLst.addAll(v);
                to.put(k, newLst);
            }
        });
    }

    private Map<String, Map<Integer, List<Integer>>> getAltMap(AlternateCount input) {
        if (null == input || null == input.getAltMapMap() || input.getAltMapMap().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Map<Integer, List<Integer>>> map = new HashMap<>();
        input.getAltMapMap().forEach((k, v) -> map.put(k, getAllels(v)));
        return map;
    }

    private Map<Integer, List<Integer>> getAllels(AlleleMap input) {
        if (null == input) {
            return Collections.emptyMap();
        }
        Map<Integer, List<Integer>> map = new HashMap<>(input.getAlternateCount());
        input.getAlternateMap().forEach((k, v) -> map.put(Integer.valueOf(k), getCollection(v)));
        return map;
    }

    private List<Integer> getCollection(SampleList list) {
        if (null == list) {
            return Collections.emptyList();
        }
        return new ArrayList<>(list.getSampleIdsList());
    }
}
