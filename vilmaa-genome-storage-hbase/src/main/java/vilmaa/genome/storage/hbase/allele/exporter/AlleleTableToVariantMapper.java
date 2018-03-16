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

package vilmaa.genome.storage.hbase.allele.exporter;

import vilmaa.genome.storage.hbase.VariantHbaseUtil;
import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToVariantConverter;
import vilmaa.genome.storage.hbase.filter.ExportFilters;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.hadoop.variant.exporters.AnalysisToFileMapper;

import java.io.IOException;
import java.util.*;

import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;

/**
 * Maps HBase entries to Variant objects and allows to filter on cohort specific OPR & MAF values.
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableToVariantMapper extends AnalysisToFileMapper {

    private volatile HBaseAlleleCountsToVariantConverter countsToVariantConverter;
    private ExportFilters filters;
    private Set<String> validCohorts;
    private String studyName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        countsToVariantConverter = new HBaseAlleleCountsToVariantConverter(getHelper(), getStudyConfiguration());
        this.countsToVariantConverter.setParseStatistics(true);
        this.countsToVariantConverter.setParseAnnotations(true);
        this.countsToVariantConverter.setReturnSamples(this.returnedSamples);
        this.countsToVariantConverter.setStudyNameAsStudyId(true);
        this.filters = ExportFilters.build(
                getStudyConfiguration(), context.getConfiguration(), getHelper().getColumnFamily());
        // Cohort name to Cohort ID
        studyName = getStudyConfiguration().getStudyName();
        validCohorts = new HashSet<>(Arrays.asList(
                context.getConfiguration().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, StudyEntry.DEFAULT_COHORT)
        ));
        getLog().info("Only export stats for {} ...", this.validCohorts);
        this.countsToVariantConverter.setCohortWhiteList(validCohorts);
    }

    protected boolean isMetaRow(Result value) {
        if (super.isMetaRow(value)) {
            return true;
        }
        Variant variant = VariantHbaseUtil.inferAndSetType(getHelper().extractVariantFromVariantRowKey(value.getRow()));
        // FALSE to keep the entry!!!
        return !this.filters.pass(value, variant);
    }

    @Override
    protected Variant convertToVariant(Result value) {
        Variant variant = this.countsToVariantConverter.convert(value);
        StudyEntry se = variant.getStudy(studyName);
        Map<String, VariantStats> cleanStats = new HashMap<>();
        Map<String, VariantStats> stats = se.getStats();
        stats.forEach((k, v) -> {
            if (validCohorts.contains(k)) {
                cleanStats.put(k, v);
            }
        });
        se.setStats(cleanStats);
        FileEntry file = se.getFiles().get(0);
        file.setAttributes(new HashMap<>()); // overwrite attributes
        return variant;
    }
}
