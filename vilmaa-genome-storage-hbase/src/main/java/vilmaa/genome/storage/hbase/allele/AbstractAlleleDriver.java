package vilmaa.genome.storage.hbase.allele;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 15/02/2017.
 */
public abstract class AbstractAlleleDriver  extends AbstractAnalysisTableDriver{
    public static final String CONFIG_COUNT_TABLE = "vilmaa.genome.allele.count.table.name";
    private String countTable;


    public AbstractAlleleDriver() { /* nothing */ }

    public AbstractAlleleDriver(Configuration conf) {
        super(conf);
    }

    public String getCountTable() {
        return countTable;
    }

    @Override
    protected void parseAndValidateParameters() {
        countTable = getConf().get(CONFIG_COUNT_TABLE, StringUtils.EMPTY);
        if (StringUtils.isBlank(countTable)) {
            throw new IllegalStateException("Count table parameter required: " + CONFIG_COUNT_TABLE);
        }
    }

    @Override
    protected void checkTablesExist(GenomeHelper genomeHelper, String... tables) {
        super.checkTablesExist(genomeHelper, this.countTable);
        super.checkTablesExist(genomeHelper, tables);
    }

    public static String buildCommandLineArgs(String server, String archive, String countTable, String analysisTable, int studyId,
                                              List<Integer> fileIds, Map<String, Object> other) {
        StringBuilder stringBuilder = new StringBuilder().append(server).append(' ').append(archive).append(' ')
                .append(analysisTable).append(' ').append(studyId).append(' ');

        stringBuilder.append(fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        stringBuilder.append(" ").append(CONFIG_COUNT_TABLE).append(" ").append(countTable);
        ArchiveDriver.addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
    }

}
