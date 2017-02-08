package diva.genome.storage.hbase.allele.count;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedIntArray;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 05/02/2017.
 */
public class HBaseToAlleleCountConverter {
    public static final char REFERENCE_PREFIX_CHAR = 'R';
    public static final char FILTER_FAIL_CHAR = 'F';
    public static final char FILTER_PASS_CHAR = 'P';
    public static final char VARIANT_PREFIX_CHAR= 'V';

    public AlleleCountPosition convert(Result result) {
        try {
            AlleleCountPosition calc = new AlleleCountPosition();
            for (Cell cell : result.rawCells()) {
                addAlleleCounts(calc, cell);
            }
            return calc;
        } catch (SQLException e) {
            throw new IllegalStateException("Problems parsing data", e);
        }
    }

    private List<Integer> getValueAsList(Cell cell) throws SQLException {

        Array abc = (Array) PUnsignedIntArray.INSTANCE.toObject(CellUtil.cloneValue(cell));
        int[] coll = null;
        if (null != abc) {
            coll = (int[]) abc.getArray();
        }
        if (null == coll) {
            coll = new int[0];
        }
        return Arrays.stream(coll).boxed().collect(Collectors.toList());
    }

    private void addAlleleCounts(AlleleCountPosition calc, Cell cell) throws SQLException {
        String column = Bytes.toString(CellUtil.cloneQualifier(cell));
        switch (column.charAt(0)) {
            case FILTER_FAIL_CHAR:
                getValueAsList(cell).forEach(i -> calc.getNotPass().add(i));
                break;
            case REFERENCE_PREFIX_CHAR:
                Integer allele = Integer.valueOf(column.substring(1));
                calc.getReference().put(allele, getValueAsList(cell));
                break;
            case VARIANT_PREFIX_CHAR:
                if (StringUtils.contains(column, '_')) {
                    String[] split = column.substring(1).split("_", 2);
                    allele = Integer.valueOf(split[1]);
                    calc.getAltMap().computeIfAbsent(split[0], k -> new HashMap<>())
                            .put(allele, getValueAsList(cell));
                } else {
                    allele = Integer.valueOf(column.substring(1));
                    calc.getAlternate().put(allele, getValueAsList(cell));
                }
                break;
            default:
                // do nothing
                break;
        }
    }

}
