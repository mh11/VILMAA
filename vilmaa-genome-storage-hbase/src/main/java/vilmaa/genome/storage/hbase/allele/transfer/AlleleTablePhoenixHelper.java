/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package vilmaa.genome.storage.hbase.allele.transfer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.STATS_PREFIX;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Helper class to define Suffixes for cohort specific columns.
 * Created by mh719 on 30/01/2017.
 */
public class AlleleTablePhoenixHelper {

    private static final String OPR_SUFFIX = "_OPR";
    private static final String CR_SUFFIX = "_CR";
    private static final String PR_SUFFIX = "_PR";
    private final VariantPhoenixHelper helper;

    public AlleleTablePhoenixHelper(GenomeHelper genomeHelper) {
        this.helper = new VariantPhoenixHelper(genomeHelper);
    }

    public VariantPhoenixHelper getHelper() {
        return helper;
    }

    public void registerNewStudy(Connection con, String table) throws SQLException {
        this.registerNewStudy(con, table, -1);
    }
    public void registerNewStudy(Connection con, String table, int studyId) throws SQLException {
        getHelper().createTableIfNeeded(con, table);

        addColumns(con, table, studyId, PUnsignedInt.INSTANCE,
                AlleleCountToHBaseConverter.FILTER_PASS,
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.REFERENCE_PREFIX, 2)) //HOM_REF
                );

        addColumns(con, table, studyId, PUnsignedIntArray.INSTANCE,
                AlleleCountToHBaseConverter.FILTER_FAIL,
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.REFERENCE_PREFIX, 1)), // REF_HET
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.REFERENCE_PREFIX, -1)), // NO CALL
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.VARIANT_PREFIX, 1)), // VAR HET
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.VARIANT_PREFIX, 2)), // VAR HOM
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.VARIANT_PREFIX, HBaseAlleleCalculator.DEL_SYMBOL, 1)), // DEL overlap
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.VARIANT_PREFIX, HBaseAlleleCalculator.DEL_SYMBOL, 2)),
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.VARIANT_PREFIX, HBaseAlleleCalculator.INS_SYMBOL, 1)), // INS overlap
                Bytes.toString(AlleleCountToHBaseConverter.buildQualifier(AlleleCountToHBaseConverter.VARIANT_PREFIX, HBaseAlleleCalculator.INS_SYMBOL, 2))
                );
        con.commit();
    }

    public void addColumns(Connection con, String tableName, int studyId, PDataType<?> dataType, String ... columns)
            throws SQLException {
        for (String col : columns) {
            String builtColumnName = col; // possibility to add study ID
            String sql = getPhoenixHelper().buildAlterAddColumn(tableName, builtColumnName, dataType.getSqlTypeName());
            getPhoenixHelper().execute(con, sql);
        }
    }

    public PhoenixHelper getPhoenixHelper() {
        return getHelper().getPhoenixHelper();
    }

    public void createVariantIndexes(Connection con, String tableName) throws SQLException {
        List<PhoenixHelper.Index> indices = getIndices(tableName);
        getPhoenixHelper().createIndexes(con, tableName, indices, false);
    }

    public static List<PhoenixHelper.Index> getIndices(String tableName) {
        TableName table = TableName.valueOf(tableName);
        List<PhoenixHelper.Column> defaultInclude = Arrays.asList(GENES, SO);
        return Arrays.asList(
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(PHASTCONS), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(PHYLOP), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(GERP), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(CADD_RAW), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(CADD_SCALLED), defaultInclude),
                // Index the min value
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList("\"" + POLYPHEN + "\"[1]"), defaultInclude),
                // Index the max value
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList("\"" + SIFT + "\"[2]"), defaultInclude),
                new PhoenixHelper.Index(table, PTable.IndexType.LOCAL, Arrays.asList(TYPE), defaultInclude)
        );
    }

    public static PhoenixHelper.Column getOprColumn(int studyId, int cohortId) {
        return PhoenixHelper.Column.build(STATS_PREFIX + studyId + "_" + cohortId + OPR_SUFFIX, PFloat.INSTANCE);
    }

    public static PhoenixHelper.Column getCallRateColumn(int studyId, int cohortId) {
        return PhoenixHelper.Column.build(STATS_PREFIX + studyId + "_" + cohortId + CR_SUFFIX, PFloat.INSTANCE);
    }

    public static PhoenixHelper.Column getPassRateColumn(int studyId, int cohortId) {
        return PhoenixHelper.Column.build(STATS_PREFIX + studyId + "_" + cohortId + PR_SUFFIX, PFloat.INSTANCE);
    }

    public void updateStatsColumns(Connection con, String tableName, StudyConfiguration studyConfiguration) throws SQLException {
        List<PhoenixHelper.Column> columns = studyConfiguration.getCohortIds().values().stream()
                .flatMap(cohortid -> getStatsColumns(studyConfiguration.getStudyId(), cohortid).stream())
                .collect(Collectors.toList());
        helper.getPhoenixHelper().addMissingColumns(con, tableName, columns, true);
    }

    private Collection<PhoenixHelper.Column> getStatsColumns(int studyId, Integer cohortId) {
        return Arrays.asList(
                getOprColumn(studyId, cohortId),
                getCallRateColumn(studyId, cohortId),
                getPassRateColumn(studyId, cohortId));
    }
}
