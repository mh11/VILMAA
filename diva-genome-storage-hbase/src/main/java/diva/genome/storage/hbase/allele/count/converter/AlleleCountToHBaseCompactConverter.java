package diva.genome.storage.hbase.allele.count.converter;

import diva.genome.storage.hbase.allele.count.AlleleCalculator;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.models.protobuf.PositionCountHBaseProto;
import diva.genome.storage.hbase.allele.models.protobuf.ReferenceCountHBaseProto;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Append;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by mh719 on 23/02/2017.
 */
public class AlleleCountToHBaseCompactConverter implements GroupedAlleleCountToHBaseAppendConverter {

    public static final int DEFAULT_REGION_SIZE = 10;
    private final byte[] refColumn;
    private final byte[] altColumn;
    private final byte[] columnFamily;
    private final AlleleCountPositionToAlleleCountHBaseProto converter;
    private final Function<Integer, Integer> groupFunction;
    private final BiFunction<String, Integer, byte[]> rowkeyFunction;

    public AlleleCountToHBaseCompactConverter(byte[] columnFamily, byte[] refColumn, byte[] altColumn) {
        this(columnFamily, refColumn, altColumn, DEFAULT_REGION_SIZE);
    }

    public AlleleCountToHBaseCompactConverter(byte[] columnFamily, byte[] refColumn, byte[] altColumn, int region) {
        // default implementation -> group by region size
        this(columnFamily, refColumn, altColumn, (pos) -> pos / region);
    }

    public AlleleCountToHBaseCompactConverter(byte[] columnFamily, byte[] refColumn, byte[] altColumn,
                                              Function<Integer, Integer> groupFunction) {
        this(columnFamily, refColumn, altColumn, groupFunction, (k, v) -> buildRowKey(k, v));
    }

    public AlleleCountToHBaseCompactConverter(byte[] columnFamily, byte[] refColumn, byte[] altColumn,
                                              Function<Integer, Integer> groupFunction,
                                              BiFunction<String, Integer, byte[]> rowkeyFunction) {
        this.converter = new AlleleCountPositionToAlleleCountHBaseProto();
        this.columnFamily = columnFamily;
        this.refColumn = refColumn;
        this.altColumn = altColumn;
        this.groupFunction = groupFunction;
        this.rowkeyFunction = rowkeyFunction;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getRefColumn() {
        return refColumn;
    }

    public byte[] getAltColumn() {
        return altColumn;
    }

    @Override
    public Collection<Append> convert(String chromosome, AlleleCalculator calculator) {
        Map<Integer, AlleleCountPosition> referenceMap = calculator.buildReferenceMap();
        Map<Integer, Map<String, AlleleCountPosition>> variantMap = calculator.buildVariantMap();
        List<Append> appends = new ArrayList<>();
        Map<Integer, List<PositionCountHBaseProto>> refMap = convertToRefProto(referenceMap);
        Map<Integer, List<PositionCountHBaseProto>> altMap = convertToAltProto(variantMap);
        Set<Integer> group = new HashSet<>();
        group.addAll(refMap.keySet());
        group.addAll(altMap.keySet());
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        group.forEach((key) -> {
            byte[] rowKey = rowkeyFunction.apply(chromosome, key);
            Append append = new Append(rowKey);
            List<PositionCountHBaseProto> refs = refMap.getOrDefault(key, Collections.emptyList());
            List<PositionCountHBaseProto> alts = altMap.getOrDefault(key, Collections.emptyList());
            try {
                byteStream.reset();
                for (PositionCountHBaseProto proto : refs) {
                    proto.writeDelimitedTo(byteStream);
                }
                append.add(getColumnFamily(), getRefColumn(), byteStream.toByteArray());

                byteStream.reset();
                for (PositionCountHBaseProto proto : alts) {
                    proto.writeDelimitedTo(byteStream);
                }
                append.add(getColumnFamily(), getAltColumn(), byteStream.toByteArray());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            appends.add(append);
        });
        return appends;
    }

    public static byte[] buildRowKey(String chromosome, Integer groupPosition) {
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(Integer.valueOf(chromosome.length())).intValue() +
                QueryConstants.SEPARATOR_BYTE_ARRAY.length + PUnsignedInt.INSTANCE.estimateByteSize(Integer.valueOf(groupPosition));
        byte[] rk = new byte[size];
        byte offset = 0;
        int var7 = offset + PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
        rk[var7++] = 0;
        PUnsignedInt.INSTANCE.toBytes(Integer.valueOf(groupPosition), rk, var7);
        return rk;
    }

    private Map<Integer, List<PositionCountHBaseProto>> convertToAltProto(Map<Integer, Map<String, AlleleCountPosition>> variantMap) {
        Map<Integer, List<PositionCountHBaseProto>> grouped = new HashMap<>();
        variantMap.forEach((pos, alts) -> {
            Integer group = this.calculateGroupPosition(pos);
            List<PositionCountHBaseProto> lst = grouped.computeIfAbsent(group, (k) -> new ArrayList<>());
            alts.forEach((id, count) -> {
                PositionCountHBaseProto build = convertToProto(pos, id, count).build();
                lst.add(build);
            });
        });
        return grouped;
    }

    public Map<Integer, List<PositionCountHBaseProto>> convertToRefProto(Map<Integer, AlleleCountPosition> referenceMap) {
        Map<Integer, List<PositionCountHBaseProto>> grouped = new HashMap<>();
        referenceMap.forEach((pos, count) -> {
            Integer group = this.calculateGroupPosition(pos);
            PositionCountHBaseProto build = convertToProto(pos, StringUtils.EMPTY, count).build();
            grouped.computeIfAbsent(group, (k) -> new ArrayList<>()).add(build);
        });
        return grouped;
    }

    public PositionCountHBaseProto.Builder convertToProto(Integer position, String id, AlleleCountPosition count) {
        ReferenceCountHBaseProto.Builder countProto = converter.convertToBuilder(count);
        PositionCountHBaseProto.Builder builder = PositionCountHBaseProto.newBuilder().setStart(position);
        if (StringUtils.isNotEmpty(id)) {
            builder = builder.setVariantId(id); // shouldn't make much difference
        }
        return builder.setAlleleCount(countProto);
    }

    @Override
    public Integer calculateGroupPosition(Integer position) {
        return this.groupFunction.apply(position);
    }
}
