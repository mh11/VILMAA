package diva.genome.storage.hbase.allele.count.region;

import diva.genome.storage.hbase.allele.count.AlleleInfo;
import diva.genome.storage.hbase.allele.models.protobuf.ARSEntry;
import diva.genome.storage.hbase.allele.models.protobuf.AlleleRegion;
import diva.genome.util.RegionImpl;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static diva.genome.storage.hbase.allele.count.region.AlleleRegionStoreToHBaseAppendConverter.*;

/**
 * Reads from HBase Result and adds to an existing or creates a new {@link AlleleRegionStore} with the stored allele counts.
 * Created by mh719 on 19/03/2017.
 */
public class HBaseToAlleleRegionStoreConverter {

    private final byte[] columnFamily;
    private final byte[] columnName;
    private final Function<byte[], Variant> keyConverter;
    private volatile int regionSize;

    public HBaseToAlleleRegionStoreConverter(GenomeHelper helper, int regionSize) {
        this(helper.getColumnFamily(), helper.getStudyId(), regionSize, (rowkey) -> helper.extractVariantFromVariantRowKey(rowkey));
    }
    public HBaseToAlleleRegionStoreConverter(byte[] columnFamily, int studyId, int regionSize, Function<byte[], Variant> convert) {
        this.columnFamily = columnFamily;
        this.columnName = Bytes.toBytes(studyId);
        this.regionSize = regionSize;
        this.keyConverter = convert;
    }

    public void setRegionSize(int regionSize) {
        this.regionSize = regionSize;
    }

    public int getRegionSize() {
        return regionSize;
    }

    public AlleleRegionStore convert(Result result) {
        Integer start = this.keyConverter.apply(result.getRow()).getStart();
        int endInclusive = start + regionSize - 1;
        AlleleRegionStore store = new AlleleRegionStore(start, endInclusive);
        convert(store, result);
        return store;
    }

    public void convert(AlleleRegionStore store, Result result) {
        Integer start = this.keyConverter.apply(result.getRow()).getStart();
        int endInclusive = start + regionSize - 1;
        try {
            fillFromBytes(store, start, endInclusive, result.getValue(this.columnFamily, this.columnName));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void fillFromBytes(AlleleRegionStore store, int regStart, int regEnd, byte[] bytes) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        AlleleRegion region = AlleleRegion.parseDelimitedFrom(inputStream);
        while (region != null) {
            update(store, regStart, regEnd, region);
            region = AlleleRegion.parseDelimitedFrom(inputStream);
        }
    }

    private void update(AlleleRegionStore store, int regStart, int regEnd, AlleleRegion region) {
        updateVarCalls(store, regStart, region.getVariationMap());
        updateNoCalls(store, regStart, regEnd, region.getNoCallMap());
        updateRefCall(store, regStart, regEnd, region.getReferenceMap());
    }

    private void updateRefCall(AlleleRegionStore store, int regStart, int regEnd, Map<Boolean, ARSEntry> noCallMap) {
        noCallMap.forEach((pass, e1) -> {
            e1.getEntryMap().forEach((type, e2) -> {
                e2.getEntryMap().forEach((count, e3) -> {
                    e3.getEntryMap().forEach((start, e4) -> {
                        int genomestart = start + regStart;
                        e4.getEntryMap().forEach((end, e5) -> {
                            int genomeend = regEnd - end;
                            e5.getEntryMap().forEach((depth, e6) -> {
                                List<Integer> sampleIdsList = e6.getSampleIdsList();
                                AlleleInfo alleleInfo = new AlleleInfo(count, depth,
                                        sampleIdsList, AlleleInfo.getReferenceAllele(),
                                        parseType(type), pass);
                                store.add(new RegionImpl<>(alleleInfo, genomestart, genomeend));
                            });
                        });
                    });
                });
            });
        });
    }

    private void updateNoCalls(AlleleRegionStore store, int regStart, int regEnd, Map<Boolean, ARSEntry> noCallMap) {
        noCallMap.forEach((pass, e1) -> {
            e1.getEntryMap().forEach((type, e2) -> {
                e2.getEntryMap().forEach((count, e3) -> {
                    e3.getEntryMap().forEach((depth, e4) -> {
                        e4.getEntryMap().forEach((start, e5) -> {
                            int genomestart = start + regStart;
                            e5.getEntryMap().forEach((end, e6) -> {
                                int genomeend = regEnd - end;
                                List<Integer> sampleIdsList = e6.getSampleIdsList();
                                AlleleInfo alleleInfo = new AlleleInfo(count, depth,
                                        sampleIdsList, AlleleInfo.getNoCallAllele(),
                                        parseType(type), pass);
                                store.add(new RegionImpl<>(alleleInfo, genomestart, genomeend));
                            });
                        });
                    });
                });
            });
        });
    }

    private void updateVarCalls(AlleleRegionStore store, int regStart, Map<Boolean, ARSEntry> variationMap) {
        variationMap.forEach((pass, e1) -> {
            e1.getEntryMap().forEach((type, e2) -> {
                e2.getEntryMap().forEach((count, e3) -> {
                    e3.getEntryMap().forEach((depth, e4) -> {
                        e4.getEntryMap().forEach((start, e5) -> {
                            int genomestart = start + regStart;
                            if (genomestart < regStart && store.getTargetRegion().getStart() < regStart) {
                                return; // already loaded by previous region for this store.
                            }
//                            e5.getEntryMap().forEach((end, e6) -> {
//                                int genomeend = end == 0 ? start : end + regStart;
                                e5.getVarsMap().forEach((vid, e7) -> {
                                    List<Integer> sampleIdsList = e7.getSampleIdsList();
                                    String[] id = parseId(vid);
                                    AlleleInfo alleleInfo = new AlleleInfo(count, depth, sampleIdsList, id, parseType(type), pass);
                                    store.add(new RegionImpl<>(alleleInfo, genomestart, genomestart + id[0].length() + - 1));
                                });
//                            });
                        });
                    });
                });
            });
        });
    }

    private VariantType parseType(Integer type) {
        switch (type) {
            case ARS_NO_VARIANT: return VariantType.NO_VARIATION;
            case ARS_SNV: return VariantType.SNV;
            case ARS_MNV: return VariantType.MNV;
            case ARS_INS: return VariantType.INSERTION;
            case ARS_DEL: return VariantType.DELETION;
            default:
                throw new IllegalStateException("Unkown option: " + type);
        }
    }

    private String[] parseId(String vid) {
        return AlleleInfo.parseVariantId(vid);
    }


}
