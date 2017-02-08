package diva.genome.storage.hbase.allele.count.converter;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static diva.genome.storage.hbase.allele.count.converter.AlleleCountToHBaseAppendGroupedConverter.*;

/**
 * Created by mh719 on 08/02/2017.
 */
public class HBaseAppendGroupedToAlleleCountConverter {
    private char refPrefix;
    private char varPrefix;
    private final byte[] columnFamily;
    AlleleCountHBaseProtoToAlleleCountPosition protoConverter = new AlleleCountHBaseProtoToAlleleCountPosition();

    public HBaseAppendGroupedToAlleleCountConverter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
        this.refPrefix = PREFIX_REFERENCE;
        this.varPrefix = PREFIX_VARIANT;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public Map<Integer, Pair<AlleleCountPosition, Map<String, AlleleCountPosition>>> convert(Result result) {
        if (result.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Pair<String, Integer> region = extractRegion(result.getRow());

            Map<Integer, Map<String, AlleleCountPosition>> altMap = new HashMap<>();
            Map<Integer, AlleleCountPosition> refMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                if (!Bytes.equals(CellUtil.cloneFamily(cell), getColumnFamily())) {
                    continue;
                }
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (col.charAt(0) == varPrefix) {
                    Integer position = extractVariantPosition(region.getRight(), col);
                    String variantId = extractVariantId(col);
                    AlleleCountPosition count = protoConverter.variantFromBytes(CellUtil.cloneValue(cell));
                    altMap.computeIfAbsent(position, x -> new HashMap<>()).put(variantId, count);
                }
            }
            Map<Integer, Pair<AlleleCountPosition, Map<String, AlleleCountPosition>>> map = new HashMap<>();

            for (Cell cell : result.rawCells()) {
                if (!Bytes.equals(CellUtil.cloneFamily(cell), getColumnFamily())) {
                    continue;
                }
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (col.charAt(0) == refPrefix) {
                    Integer position = extractReferencePosition(region.getRight(), col);
                    if (altMap.containsKey(position)) {
                        Map<String, AlleleCountPosition> alts = altMap.get(position);
                        if (!alts.isEmpty()) {
                            AlleleCountPosition count = protoConverter.referenceFromBytes(CellUtil.cloneValue(cell));
                            map.put(position, new ImmutablePair<>(count, alts));
                        }
                    }
                }
            }
            return map;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String extractVariantId(String col) {
        int idx = col.indexOf(':');
        if (idx < 0) {
            throw new IllegalStateException("Not ':' found in variant " + col);
        }
        return col.substring(idx+1);
    }

    private Integer extractReferencePosition(Integer region, String col) {
        return (region * 10) + Integer.valueOf(col.substring(1));
    }


    private Integer extractVariantPosition(Integer region, String col) {
        col = col.substring(1);
        int sepIdx = col.indexOf(':');
        if (sepIdx < 0) {
            throw new IllegalStateException("Not ':' found in variant " + col);
        }
        return (region * 10) + Integer.valueOf(col.substring(0, sepIdx));
    }

    public Pair<String, Integer> extractRegion(byte[] region) {
        int chrPosSeparator = ArrayUtils.indexOf(region, (byte) 0);
        String chromosome = (String) PVarchar.INSTANCE.toObject(region, 0, chrPosSeparator, PVarchar.INSTANCE);
        Integer intSize = PUnsignedInt.INSTANCE.getByteSize();
        int position = (Integer) PUnsignedInt.INSTANCE.toObject(region, chrPosSeparator + 1, intSize, PUnsignedInt.INSTANCE);
        return new ImmutablePair<>(chromosome, position);
    }



}
