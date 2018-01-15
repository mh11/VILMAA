package vilmaa.genome.analysis.models.variant.stats;

import vilmaa.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Convert Stati
 * Created by mh719 on 28/03/2017.
 */
public class HBaseToVariantStatisticsConverter {

    private final byte[] columnFamily;
    private final Integer studyId;
    private final Set<Integer> cohortIds;
    private volatile HBaseToVariantStatsConverter statsConverter;

    public HBaseToVariantStatisticsConverter(GenomeHelper gh, byte[] columnFamily, Integer studyId, Collection<Integer> cohortIds) {
        this(new HBaseToVariantStatsConverter(gh), columnFamily, studyId, cohortIds);
    }

    public HBaseToVariantStatisticsConverter(HBaseToVariantStatsConverter statsConverter, byte[] columnFamily, Integer studyId, Collection<Integer> cohortIds) {
        this.columnFamily = columnFamily;
        this.studyId = studyId;
        this.cohortIds = new HashSet<>(cohortIds);
        this.statsConverter = statsConverter;
    }

    protected Float extractSpecificColumn(Result result, PhoenixHelper.Column column) {
        if (!result.containsColumn(this.columnFamily, column.bytes())) {
            return null;
        }
        Object o = column.getPDataType().toObject(CellUtil.cloneValue(result.getColumnLatestCell(this.columnFamily, column.bytes())));
        if (o instanceof Float) {
            return (Float) o;
        }
        throw new IllegalStateException("Unexpected type " + o.getClass() + " of value " + o + " for " + column.column());
    }

    public Map<Integer, Map<Integer, VariantStatistics>> convert(Result result) {
        Map<Integer, Map<Integer, VariantStatistics>> convert = toStats(statsConverter.convert(result));
        convert.forEach((studyId, e) -> e.forEach((cohortId, stats) -> {
                stats.setOverallPassRate(extractSpecificColumn(result, AlleleTablePhoenixHelper.getOprColumn(studyId, cohortId)));
                stats.setPassRate(extractSpecificColumn(result, AlleleTablePhoenixHelper.getPassRateColumn(studyId, cohortId)));
                stats.setCallRate(extractSpecificColumn(result, AlleleTablePhoenixHelper.getCallRateColumn(studyId, cohortId)));
            }
        ));
        return convert;
    }

    public Map<Integer, Map<Integer, VariantStatistics>> convert(ResultSet result) {
        Map<Integer, Map<Integer, VariantStatistics>> convert = toStats(statsConverter.convert(result));
        convert.forEach((studyId, e) -> e.forEach((cohortId, stats) -> {
                stats.setOverallPassRate(extractFloat(result, AlleleTablePhoenixHelper.getOprColumn(studyId, cohortId)));
                stats.setPassRate(extractFloat(result, AlleleTablePhoenixHelper.getPassRateColumn(studyId, cohortId)));
                stats.setCallRate(extractFloat(result, AlleleTablePhoenixHelper.getCallRateColumn(studyId, cohortId)));
            }
        ));
        return convert;
    }

    private Float extractFloat(ResultSet result, PhoenixHelper.Column column) {
        try {
            ResultSetMetaData metaData = result.getMetaData();
            int oprcolumnNumber = -1;
            for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                if (metaData.getColumnName(i).equals(column.column())) {
                    oprcolumnNumber = i;
                    break;
                }
            }
            if (oprcolumnNumber < 0) {
                return null; // no column name
            }
            byte[] aBytes = result.getBytes(oprcolumnNumber);
            if (null == aBytes || aBytes.length == 0) {
                return null; // no data
            }
            return (Float) column.getPDataType().toObject(aBytes);
        } catch (SQLException e) {
            throw new IllegalStateException("Problem with column " + column.column());
        }
    }

    private Map<Integer, Map<Integer, VariantStatistics>> toStats(Map<Integer, Map<Integer, VariantStats>> map) {
        Map<Integer, Map<Integer, VariantStatistics>> retmap = new HashMap<>();
        map.forEach((studyId, e) -> e.forEach((cohortId, stats) -> {
            VariantStatistics vstats = new VariantStatistics(stats);
            retmap.computeIfAbsent(studyId, k -> new HashMap<>()).put(cohortId, vstats);
        }));
        // ensure all cohort IDs are filled.
        this.cohortIds.forEach(cid -> retmap.computeIfAbsent(this.studyId, k -> new HashMap<>())
                .computeIfAbsent(cid, k -> new VariantStatistics()));
        return retmap;
    }
}
