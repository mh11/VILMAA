package vilmaa.genome.storage.hbase.allele.count;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Convert HBase count data into {@link AlleleCountPosition} object.
 * Supports {@link ResultSet} and {@link Result} provided by Phoenix query or Scan.
 *
 * Created by mh719 on 05/02/2017.
 */
public class HBaseToAlleleCountConverter {
    public static final char REFERENCE_PREFIX_CHAR = 'R';
    public static final char FILTER_FAIL_CHAR = 'F';
    public static final char FILTER_PASS_CHAR = 'P';
    public static final char VARIANT_PREFIX_CHAR= 'V';

    private volatile Set<Integer> validIds = new HashSet<>();
    private volatile boolean useIdFilter = false;

    public HBaseToAlleleCountConverter() {
        this.useIdFilter = false;
    }

    /**
     * Add valid IDs to filter on. Default: all IDs are allowed. Once an ID list is provided,
     * only IDs in the list are returned / added.
     * @param validIds
     */
    public void addValidIds(Collection<Integer> validIds) {
        this.validIds.addAll(validIds);
        this.useIdFilter = true;
    }

    public void setValidIds(Collection<Integer> validIds) {
        this.validIds.clear();
        this.addValidIds(validIds);
    }

    public AlleleCountPosition convert(ResultSet resultSet) throws SQLException {
        AlleleCountPosition calc = new AlleleCountPosition();
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (columnName != null && !columnName.isEmpty()) {
                byte[] bytes = resultSet.getBytes(columnName);
                if (bytes != null && !isVariantIdColumns(columnName)) {
                    addAlleleCounts(calc, columnName, () -> bytes);
                }
            }
        }
        return calc;
    }

    private boolean isVariantIdColumns(String columnName) {
        return VariantPhoenixHelper.VariantColumn.CHROMOSOME.column().equals(columnName) ||
                VariantPhoenixHelper.VariantColumn.POSITION.column().equals(columnName) ||
                VariantPhoenixHelper.VariantColumn.REFERENCE.column().equals(columnName) ||
                VariantPhoenixHelper.VariantColumn.ALTERNATE.column().equals(columnName) ||
                VariantPhoenixHelper.VariantColumn.TYPE.column().equals(columnName);
    }


    public AlleleCountPosition convert(Result result) {
        try {
            AlleleCountPosition calc = new AlleleCountPosition();
            if (result.isEmpty()) {
		return calc;
            }
            for (Cell cell : result.rawCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                addAlleleCounts(calc, column, () -> CellUtil.cloneValue(cell));
            }
            return calc;
        } catch (SQLException e) {
            throw new IllegalStateException("Problems parsing data", e);
        }
    }

    public static List<Integer> getValueAsList(byte[] bytes) throws SQLException {
        return getValueAsList(bytes, (i) -> true);
    }

    public static List<Integer> getValueAsList(byte[] bytes, Predicate<Integer> filter) throws SQLException {
        Array abc = (Array) PUnsignedIntArray.INSTANCE.toObject(bytes);
        int[] coll = null;
        if (null != abc) {
            coll = (int[]) abc.getArray();
        }
        if (null == coll) {
            coll = new int[0];
        }
        return Arrays.stream(coll).boxed().filter(i -> filter.test(i)).collect(Collectors.toList());
    }

    private List<Integer> getFilteredValues(byte[] bytes) throws SQLException {
        if (this.useIdFilter) return getValueAsList(bytes, (i) -> this.validIds.contains(i));
        return getValueAsList(bytes);
    }

    private void addAlleleCounts(AlleleCountPosition calc, String columnName, Supplier<byte[]> byteSupplier)
            throws SQLException {
        switch (columnName.charAt(0)) {
            case FILTER_FAIL_CHAR:
                getFilteredValues(byteSupplier.get()).forEach(i -> calc.getNotPass().add(i));
                break;
            case REFERENCE_PREFIX_CHAR:
                Integer allele = Integer.valueOf(columnName.substring(1));
                calc.getReference().put(allele, getFilteredValues(byteSupplier.get()));
                break;
            case VARIANT_PREFIX_CHAR:
                if (StringUtils.contains(columnName, '_')) {
                    String[] split = columnName.substring(1).split("_", 2);
                    allele = Integer.valueOf(split[1]);
                    calc.getAltMap().computeIfAbsent(split[0], k -> new HashMap<>())
                            .put(allele, getFilteredValues(byteSupplier.get()));
                } else {
                    allele = Integer.valueOf(columnName.substring(1));
                    calc.getAlternate().put(allele, getFilteredValues(byteSupplier.get()));
                }
                break;
            default:
                // do nothing
                break;
        }
    }

}
