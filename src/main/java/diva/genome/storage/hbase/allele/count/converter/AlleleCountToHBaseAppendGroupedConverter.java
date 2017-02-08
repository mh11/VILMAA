package diva.genome.storage.hbase.allele.count.converter;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.apache.commons.math.exception.OutOfRangeException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleCountToHBaseAppendGroupedConverter {

    public static final char PREFIX_REFERENCE = 'Y';
    public static final char PREFIX_VARIANT = 'Z';
    private String refPrefix;
    private String varPrefix;
    private final byte[] columnFamily;
    AlleleCountPositionToAlleleCountHBaseProto protoConverter = new AlleleCountPositionToAlleleCountHBaseProto();

    public AlleleCountToHBaseAppendGroupedConverter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
        this.refPrefix = PREFIX_REFERENCE + "";
        this.varPrefix = PREFIX_VARIANT + "";
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public Collection<Append> convert(String chromosome, Map<Integer, AlleleCountPosition> referenceMap, Map<Integer, Map<String, AlleleCountPosition>> variantMap) {
        Map<Integer, Map<Integer, AlleleCountPosition>> regionMap = new HashMap<>();
        Map<Integer, Map<Integer, Map<String, AlleleCountPosition>>> regionVarMap = new HashMap<>();
        // group by position
        referenceMap.forEach((k, v) -> regionMap.computeIfAbsent(calcPosition(k), x -> new HashMap<>()).put(calcRemaining(k), v));
        variantMap.forEach((k, v) -> regionVarMap.computeIfAbsent(calcPosition(k), x -> new HashMap<>()).put(calcRemaining(k), v));

        Set<Integer> allPositions = new HashSet<>();
        allPositions.addAll(regionMap.keySet());
        allPositions.addAll(regionVarMap.keySet());

        List<Append> appends = new ArrayList<>();
        allPositions.forEach(refPos -> {
            AtomicBoolean send = new AtomicBoolean(false);
            Append append = new Append(buildRowKey(chromosome, refPos));
            addReferenceData(regionMap.get(refPos), send, append);
            addVariantData(regionVarMap.get(refPos), send, append);
            if (send.get()) {
                appends.add(append);
            }
        });
        return appends;
    }

    private void addVariantData(Map<Integer, Map<String, AlleleCountPosition>> altMap, AtomicBoolean send,
                                Append append) {
        if (null == altMap || altMap.isEmpty()) {
            return;
        }
        altMap.forEach((sub, map) -> map.forEach((varId, count) -> {
            if (protoConverter.hasVariantData(count)) {
                send.set(true);
                addVariantData(sub, varId, count, append);
            }
        }));
    }

    private void addVariantData(Integer prefix, String varId, AlleleCountPosition data, Append append) {
        byte[] bytes = protoConverter.variantToBytes(data);
        byte[] col = buildVariantColumn(prefix, varId);
        append.add(getColumnFamily(), col, bytes);
    }


    private void addReferenceData(Map<Integer, AlleleCountPosition> refMap, AtomicBoolean send, Append append) {
        if (null == refMap) {
            return;
        }
        refMap.forEach((prefix, count) -> {
            if (protoConverter.hasReferenceData(count)) {
                send.set(true);
                byte[] bytes = protoConverter.referenceToBytes(count);
                byte[] col = buildReferenceColumn(prefix);
                append.add(getColumnFamily(), col, bytes);
            }
        });
    }

    private byte[] buildVariantColumn(Integer col, String varId) {
        return Bytes.toBytes(this.varPrefix + col.toString() + ":" + varId);
    }

    private byte[] buildReferenceColumn(Integer col) {
        return Bytes.toBytes(this.refPrefix + col.toString());
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


    public Integer calcRemaining(Integer k) {
        int i = k % 10;
        if (i > 9 || i < 0) {
            throw new OutOfRangeException(i, 0, 9);
        }
        return i;
    }

    public int calcPosition(Integer k) {
        return k/10;
    }

}
