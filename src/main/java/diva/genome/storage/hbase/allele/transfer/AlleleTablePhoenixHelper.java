package diva.genome.storage.hbase.allele.transfer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.sql.Connection;
import java.sql.SQLException;

import static diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter.*;
import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.*;

/**
 * Created by mh719 on 30/01/2017.
 */
public class AlleleTablePhoenixHelper {

    private final VariantPhoenixHelper helper;

    public AlleleTablePhoenixHelper(GenomeHelper genomeHelper) {
        this.helper = new VariantPhoenixHelper(genomeHelper);
    }

    public VariantPhoenixHelper getHelper() {
        return helper;
    }

    public void registerNewStudy(Connection con, String table) throws SQLException {
        getHelper().createTableIfNeeded(con, table);

        addColumns(con, table, PUnsignedInt.INSTANCE,
                FILTER_PASS,
                Bytes.toString(buildQualifier(REFERENCE_PREFIX, 2)) //HOM_REF
                );

        addColumns(con, table, PUnsignedIntArray.INSTANCE,
                FILTER_FAIL,
//                Bytes.toString(buildQualifier(REFERENCE_PREFIX, 1)), // REF_HET
//                Bytes.toString(buildQualifier(REFERENCE_PREFIX, -1)), // NO CALL
                Bytes.toString(buildQualifier(VARIANT_PREFIX, 1)), // VAR HET
                Bytes.toString(buildQualifier(VARIANT_PREFIX, 2)), // VAR HOM
                Bytes.toString(buildQualifier(VARIANT_PREFIX, DEL_SYMBOL, 1)), // DEL overlap
                Bytes.toString(buildQualifier(VARIANT_PREFIX, DEL_SYMBOL, 2)),
                Bytes.toString(buildQualifier(VARIANT_PREFIX, INS_SYMBOL, 1)), // INS overlap
                Bytes.toString(buildQualifier(VARIANT_PREFIX, INS_SYMBOL, 2))
                );
        con.commit();
    }

    public void addColumns(Connection con, String tableName, PDataType<?> dataType, String ... columns)
            throws SQLException {
        for (String col : columns) {
            String sql = getPhoenixHelper().buildAlterAddColumn(tableName, col, dataType.getSqlTypeName());
            getPhoenixHelper().execute(con, sql);
        }
    }

    protected PhoenixHelper getPhoenixHelper() {
        return getHelper().getPhoenixHelper();
    }
}
