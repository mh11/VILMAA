package diva.genome.storage.hbase.allele.count.region;

import com.google.protobuf.MessageLite;
import diva.genome.storage.hbase.allele.models.protobuf.*;
import diva.genome.storage.hbase.allele.models.protobuf.AlleleRegion.Builder;
import diva.genome.util.Region;
import diva.genome.util.RegionImpl;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Converts an in-memory {@link AlleleRegionStore} to an append object for HBase.
 * Created by mh719 on 19/03/2017.
 */
public class AlleleRegionStoreToHBaseAppendConverter {
    protected static final int ARS_NO_VARIANT = 0;
    protected static final int ARS_SNV = 1;
    protected static final int ARS_MNV = 2;
    protected static final int ARS_INS = 3;
    protected static final int ARS_DEL = 4;
    protected static final int DEFAULT_REGION_SIZE = 100;
    private volatile ByteArrayOutputStream bout;
    private final byte[] columnFamily;
    private final byte[] columnName;
    private volatile int regionSize;

    public AlleleRegionStoreToHBaseAppendConverter(byte[] columnFamily, int studyId) {
        this.columnFamily = columnFamily;
        this.bout = new ByteArrayOutputStream();
        this.columnName = Bytes.toBytes(studyId);
        regionSize = DEFAULT_REGION_SIZE;
    }

    public void setRegionSize(int regionSize) {
        this.regionSize = regionSize;
    }

    public byte[] toBytes(MessageLite msg) {
        bout.reset();
        try {
            msg.writeDelimitedTo(bout);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Problems during convertion from Proto to byte[]", e);
        }
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public Collection<Append> convert(String chromosome, AlleleRegionStore store) {
        List<Append> appendList = new ArrayList<>();
        Region targetRegion = store.getTargetRegion();
        for (int i = targetRegion.getStart(); i < targetRegion.getEnd(); i += getRegionSize()) {
            appendList.addAll(convert(new RegionImpl(targetRegion.getData(), i, i+getRegionSize() - 1), chromosome, store));
        }
        return appendList;
    }

    public int getRegionSize() {
        return regionSize;
    }

    public Collection<Append> convert(Region targetRegion, String chromosome, AlleleRegionStore store) {
        Builder builder = diva.genome.storage.hbase.allele.models.protobuf.AlleleRegion.newBuilder();
        builder.putAllNoCall(buildNoCall(targetRegion, store));
        builder.putAllReference(buildRefCall(targetRegion, store));
        builder.putAllVariation(buildVarCall(targetRegion, store));

        byte[] bytes = toBytes(builder.build());
        Append append = new Append(buildRowKey(chromosome, targetRegion.getMinPosition()));
        append.add(getColumnFamily(), columnName, bytes);
        return Collections.singleton(append);
    }

    private Map<Boolean,ARSEntry> buildVarCall(Region targetRegion, AlleleRegionStore store) {
        Map<Boolean,Map<VariantType, Map<Integer, Map<Integer, Map<Integer, Map<String, List<Integer>>>>>>> map = new HashMap<>();
        store.getVariation( targetRegion, r -> {
            map.computeIfAbsent(r.getData().isPass(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getType(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getCount(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getDepth(), k -> new HashMap<>())
                    .computeIfAbsent(r.getStart() - targetRegion.getStart(), k -> new HashMap<>())
//                    .computeIfAbsent( r.getStart() == r.getEnd() ? 0 : r.getEnd() - targetRegion.getStart(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getIdString(), k -> new ArrayList<>())
                    .addAll(r.getData().getSampleIds());
        });
        return buildArsVar(map);
    }

    private Map<Boolean,ARSEntry> buildNoCall(Region targetRegion, AlleleRegionStore store) {
        Map<Boolean,Map<VariantType, Map<Integer, Map<Integer, Map<Integer, Map<Integer, List<Integer>>>>>>> map = new HashMap<>();
        store.getNocall(targetRegion, r -> {
            map.computeIfAbsent(r.getData().isPass(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getType(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getCount(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getDepth(), k -> new HashMap<>())
                    .computeIfAbsent(buildStart(r.getStart(), targetRegion.getStart()), k -> new HashMap<>())
                    .computeIfAbsent(buildEnd(r.getEnd(), targetRegion.getStart(), targetRegion.getEnd()), k -> new ArrayList<>())
                    .addAll(r.getData().getSampleIds());
        });
        return buildArs(map);
    }

    private Map<Boolean,ARSEntry> buildRefCall(Region targetRegion, AlleleRegionStore store) {
        Map<Boolean,Map<VariantType, Map<Integer, Map<Integer, Map<Integer, Map<Integer, List<Integer>>>>>>> map = new HashMap<>();
        store.getReference(targetRegion, r -> {
            map.computeIfAbsent(r.getData().isPass(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getType(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getCount(), k -> new HashMap<>())
                    .computeIfAbsent(buildStart(r.getStart(), targetRegion.getStart()), k -> new HashMap<>())
                    .computeIfAbsent(buildEnd(r.getEnd(), targetRegion.getStart(), targetRegion.getEnd()), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getDepth(), k -> new ArrayList<>())
                    .addAll(r.getData().getSampleIds());
        });
        return buildArs(map);
    }


    private Map<Boolean, ARSEntry> buildArsVar(Map<Boolean,Map<VariantType, Map<Integer, Map<Integer, Map<Integer, Map<String, List<Integer>>>>>>> map) {
        Map<Boolean,ARSEntry> retmap = new HashMap<>();
        map.forEach((bool, e1) -> {
            ARSEntry.Builder b1 = ARSEntry.newBuilder();
            e1.forEach((type, e2) -> {
                ARSEntry.Builder b2 = ARSEntry.newBuilder();
                e2.forEach((k3, e3) -> {
                    ARSEntry.Builder b3 = ARSEntry.newBuilder();
//                    e3.forEach((k4, e4) -> {
//                        ARSEntry.Builder b4 = ARSEntry.newBuilder();
                        e3.forEach((k5, e5) -> {
                            ARSEntry.Builder b5 = ARSEntry.newBuilder();
                            e5.forEach((k6, e6) -> {
                                ARSEntry.Builder b6 = ARSEntry.newBuilder();
                                e6.forEach((k7,e7) -> {
                                    b6.putVars(k7, ARSEntry.newBuilder().addAllSampleIds(e7).build());
                                });
                                b5.putEntry(k6, b6.build());
                            });
                            b3.putEntry(k5, b5.build());
                        });
//                        b3.putEntry(k5, b5.build());
//                    });
                    b2.putEntry(k3, b3.build());
                });
                b1.putEntry(encodeType(type), b2.build());
            });
            retmap.put(bool, b1.build());
        });
        return retmap;
    }

    private Map<Boolean, ARSEntry> buildArs(Map<Boolean,Map<VariantType, Map<Integer, Map<Integer, Map<Integer, Map<Integer, List<Integer>>>>>>> map) {
        Map<Boolean,ARSEntry> retmap = new HashMap<>();
        map.forEach((bool, e1) -> {
            ARSEntry.Builder b1 = ARSEntry.newBuilder();
            e1.forEach((type, e2) -> {
                ARSEntry.Builder b2 = ARSEntry.newBuilder();
                e2.forEach((k3, e3) -> {
                    ARSEntry.Builder b3 = ARSEntry.newBuilder();
                    e3.forEach((k4, e4) -> {
                        ARSEntry.Builder b4 = ARSEntry.newBuilder();
                        e4.forEach((k5, e5) -> {
                            ARSEntry.Builder b5 = ARSEntry.newBuilder();
                            e5.forEach((k6, e6) -> {
                                b5.putEntry(k6, ARSEntry.newBuilder().addAllSampleIds(e6).build());
                            });
                            b4.putEntry(k5, b5.build());
                        });
                        b3.putEntry(k4, b4.build());
                    });
                    b2.putEntry(k3, b3.build());
                });
                b1.putEntry(encodeType(type), b2.build());
            });
            retmap.put(bool, b1.build());
        });
        return retmap;
    }

    private byte[] buildRowKey(String chromosome, Integer position) {
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(Integer.valueOf(chromosome.length())).intValue() +
                QueryConstants.SEPARATOR_BYTE_ARRAY.length + PUnsignedInt.INSTANCE.estimateByteSize(Integer.valueOf(position));
        byte[] rk = new byte[size];
        byte offset = 0;
        int var7 = offset + PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
        rk[var7++] = 0;
        PUnsignedInt.INSTANCE.toBytes(Integer.valueOf(position), rk, var7);
        return rk;
    }

    private int buildEnd(int regionEnd, int start, int end) {
        int rend = regionEnd - end; // store relative end going backwards
        if (rend >= 0) {
            rend = 0; // goes beyond end
        }
        return Math.abs(rend);
    }

    private int buildStart(int regionStart, int start) {
        int rstart = regionStart - start; // relative start
        if (rstart < 0) {
            rstart = 0;
        }
        return rstart;
    }

    private int encodeType(VariantType type) {
        switch (type) {
            case NO_VARIATION: return ARS_NO_VARIANT;
            case SNV:
            case SNP: return ARS_SNV;
            case MNV:
            case MNP: return ARS_MNV;
            case INDEL: throw new IllegalStateException("INDEL should be INSERTION or DELETION");
            case INSERTION: return ARS_INS;
            case DELETION: return ARS_DEL;
            default:
                throw new IllegalStateException("Not supported yet: " + type);
        }
    }
}
