package diva.genome.storage.hbase.allele.exporter;

import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.avro.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.AnalysisExportDriver.*;

/**
 * Created by mh719 on 24/02/2017.
 */
public class AlleleTableToAlleleMapper extends AbstractHBaseMapReduce<Object, Object> {

    private HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;

    private byte[] studiesRow;
    protected volatile boolean withGenotype;
    protected volatile Set<String> exportCohort;
    protected volatile Set<Integer> returnedSampleIds;

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
                if (sc.getCohortIds().containsKey(c)) {
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

        getLog().info("Export Genotype [{}] of {} samples ... ", withGenotype, returnedSampleIds.size());
        this.setHBaseAlleleCountsToAllelesConverter(buildConverter());
    }

    public HBaseAlleleCountsToAllelesConverter buildConverter() {
        HBaseAlleleCountsToAllelesConverter converter = new
                HBaseAlleleCountsToAllelesConverter(this.getHelper(), this.getStudyConfiguration());
        converter.setReturnSampleIds(this.returnedSampleIds);
        converter.setMutableSamplesPosition(true);
//        converter.set
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
        if (!isMetaRow(value)) { // ignore _METADATA row
            AllelesAvro allelesAvro = convertToVariant(value);
            if (doInclude(allelesAvro)) {
                context.write(new AvroKey<>(allelesAvro), NullWritable.get());
                context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, allelesAvro.getType().name()).increment(1);
            }
        }
    }

    protected boolean doInclude(AllelesAvro allelesAvro) {
        return validStats(allelesAvro.getStats());
    }

    /**
     * Test if variant is part of the export cohort
     * @param stats
     * @return True if the {@link VariantStats#getAltAlleleCount()} is greater one for one of the {@link #exportCohort}
     */
    protected boolean validStats(Map<String, VariantStats> stats) {
        if (null == stats) {
            return false;
        }
        for (String cohort : this.exportCohort) {
            VariantStats cohortStat = stats.get(cohort);
            if (null != cohortStat && null != cohortStat.getAltAlleleCount() && cohortStat.getAltAlleleCount() > 0) {
                return true;
            }
        }
        return false;
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


    protected AllelesAvro convertToVariant(Result value) {
        return this.getHBaseAlleleCountsToAllelesConverter().convert(value).build();
    }
}
