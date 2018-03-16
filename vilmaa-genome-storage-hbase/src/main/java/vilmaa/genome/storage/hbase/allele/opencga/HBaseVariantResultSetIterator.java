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

package vilmaa.genome.storage.hbase.allele.opencga;

import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToVariantConverter;
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
    private static Logger logger = LoggerFactory.getLogger(HBaseVariantResultSetIterator.class);

    private final HBaseAlleleCountsToVariantConverter variantConverter;

    public HBaseVariantResultSetIterator(Statement statement, ResultSet resultSet, GenomeHelper genomeHelper, StudyConfigurationManager scm, QueryOptions options) throws SQLException {
        this(statement, resultSet, genomeHelper, scm, options, Collections.emptyList());
    }

    public HBaseVariantResultSetIterator(Statement statement, ResultSet resultSet, GenomeHelper genomeHelper,
                                         StudyConfigurationManager scm, QueryOptions options, List<String>
                                                 returnedSamples) throws SQLException {
        super(statement, resultSet, genomeHelper, scm, options, returnedSamples);
        Integer studyId = scm.getStudyIds(new QueryOptions()).get(0);
        if (null == studyId || studyId < 0) {
            throw new IllegalStateException("No Study ID found for: " + studyId);
        }
        GenomeHelper.setStudyId(genomeHelper.getConf(), studyId);
        QueryResult<StudyConfiguration> queryResult = scm.getStudyConfiguration(studyId,
                new QueryOptions(StudyConfigurationManager.READ_ONLY, true));
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
