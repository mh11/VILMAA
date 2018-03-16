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
import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import vilmaa.genome.storage.hbase.filter.ExportFilters;
import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;
import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_GENOTYPE;

/**
 * Export vilmaa allele style AVRO file and allow to filter on MAF and OPR on different cohorts. <br>
 * MAF filter: passes, if one cohort has MAF greater than the value <br>
 * OPR filter: failes, if one cohort has an OPR below the value
 *
 * Created by mh719 on 24/02/2017.
 */
public class AlleleTableToAlleleMapper extends AbstractHBaseMapReduce<Object, Object> {
    private HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;

    private byte[] studiesRow;
    protected volatile boolean withGenotype;
    protected volatile Set<String> exportCohort;
    protected volatile Set<Integer> returnedSampleIds;
    private ExportFilters filters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        StudyConfiguration sc = getStudyConfiguration();
        Set<Integer> availableSamples = this.getIndexedSamples().values();
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        this.returnedSampleIds = new HashSet<>();
        withGenotype = context.getConfiguration().getBoolean(CONFIG_ANALYSIS_EXPORT_GENOTYPE, true);
        this.exportCohort = new HashSet<>(Arrays.asList(
                context.getConfiguration().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, "ALL")));
        if (withGenotype) {
            exportCohort.forEach((c) -> {
                if (!sc.getCohortIds().containsKey(c)) {
                    throw new IllegalStateException("Cohort does not exist: " + c);
                }
                Integer id = sc.getCohortIds().get(c);
                this.returnedSampleIds.addAll(sc.getCohorts().getOrDefault(id, Collections.emptySet()));
            });
        }
        Set<Integer> invalid = this.returnedSampleIds.stream()
                .filter(k -> !availableSamples.contains(k)).collect(Collectors.toSet());
        if (!invalid.isEmpty()) {
            throw new IllegalStateException("Cohort sample(s) not indexed: " + invalid);
        }

        this.filters = ExportFilters.build(
                getStudyConfiguration(), context.getConfiguration(), getHelper().getColumnFamily());

        getLog().info("Export Genotype [{}] of {} samples ... ", withGenotype, returnedSampleIds.size());
        this.setHBaseAlleleCountsToAllelesConverter(buildConverter());
    }

    public HBaseAlleleCountsToAllelesConverter buildConverter() {
        HBaseAlleleCountsToAllelesConverter converter = new
                HBaseAlleleCountsToAllelesConverter(this.getHelper(), this.getStudyConfiguration());
        converter.setReturnSampleIds(this.returnedSampleIds);
        converter.setMutableSamplesPosition(true);
        converter.setParseAnnotations(true);
        converter.setParseStatistics(true);
        converter.setCohortWhiteList(this.exportCohort);
        return converter;
    }

    public void setWithGenotype(boolean withGenotype) {
        this.withGenotype = withGenotype;
    }

    public boolean isWithGenotype() {
        return withGenotype;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException,
            InterruptedException {
        if (!isMetaRow(value) && isValid(value)) { // ignore _METADATA row
            AlleleVariant allelesAvro = convertToVariant(value);
            context.write(new AvroKey<>(allelesAvro), NullWritable.get());
            context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, allelesAvro.getType().name()).increment(1);
        }
    }

    protected boolean isValid(Result value) {
        Variant variant = VariantHbaseUtil.inferAndSetType(getHelper().extractVariantFromVariantRowKey(value.getRow()));
        return filters.pass(value, variant);
    }

    public HBaseAlleleCountsToAllelesConverter getHBaseAlleleCountsToAllelesConverter() {
        return hBaseAlleleCountsToAllelesConverter;
    }

    public void setHBaseAlleleCountsToAllelesConverter(HBaseAlleleCountsToAllelesConverter
                                                               hBaseAlleleCountsToAllelesConverter) {
        this.hBaseAlleleCountsToAllelesConverter = hBaseAlleleCountsToAllelesConverter;
    }

    protected boolean isMetaRow(Result value) {
        return Bytes.startsWith(value.getRow(), this.studiesRow);
    }


    protected AlleleVariant convertToVariant(Result value) {
        return this.getHBaseAlleleCountsToAllelesConverter().convert(value).build();
    }
}
