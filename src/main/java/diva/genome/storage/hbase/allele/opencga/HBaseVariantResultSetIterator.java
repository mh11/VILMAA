package diva.genome.storage.hbase.allele.opencga;

import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseResultSetIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HBaseVariantResultSetIterator extends VariantHBaseResultSetIterator {
    private static Logger logger = LoggerFactory.getLogger(HbaseVariantStorageEngine.class);

    private final HBaseAlleleCountsToVariantConverter variantConverter;

    public HBaseVariantResultSetIterator(Statement statement, ResultSet resultSet, GenomeHelper genomeHelper, StudyConfigurationManager scm, QueryOptions options) throws SQLException {
        this(statement, resultSet, genomeHelper, scm, options, Collections.emptyList());
    }

    public HBaseVariantResultSetIterator(Statement statement, ResultSet resultSet, GenomeHelper genomeHelper,
                                         StudyConfigurationManager scm, QueryOptions options, List<String>
                                                 returnedSamples) throws SQLException {
        super(statement, resultSet, genomeHelper, scm, options, returnedSamples);
        int studyId = genomeHelper.getStudyId();
        QueryResult<StudyConfiguration> queryResult = scm.getStudyConfiguration(studyId, new QueryOptions
                (StudyConfigurationManager.READ_ONLY, true));
        if (queryResult.getResult().isEmpty()) {
            throw new IllegalStateException("No study found for study ID: " + studyId);
        }
        StudyConfiguration sc = queryResult.first();
        logger.info("Prepare converter for study {} returning {} samples ", studyId, returnedSamples.size());
        this.variantConverter = new HBaseAlleleCountsToVariantConverter(genomeHelper, sc);
        this.variantConverter.setReturnSamples(returnedSamples);
        this.variantConverter.setMutableSamplesPosition(false);
        this.variantConverter.setStudyNameAsStudyId(true);
        this.variantConverter.setParseAnnotations(true);
        this.variantConverter.setParseStatistics(true);
    }

    @Override
    protected Variant doConvert(ResultSet result) throws SQLException {
        return convert(() -> variantConverter.convert(result));
    }
}
