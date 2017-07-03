package diva.genome.storage.hbase.allele.exporter;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.VariantHbaseUtil;
import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import diva.genome.storage.models.alleles.avro.AlleleVariant;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.AnalysisExportDriver.*;
import static diva.genome.storage.hbase.allele.exporter.ExportOprFilter.*;

/**
 * Export DIVA allele style AVRO file and allow to filter on MAF and OPR on different cohorts. <br>
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
    protected volatile Map<String, Map<PhoenixHelper.Column, Float>> chromMafFilters = new HashMap<>();
    private ExportOprFilter oprFilter;

    private Map<PhoenixHelper.Column, Float> buildFilterMap(String[] cohortArr, Float cutoff, Map<String, Integer> cohortMap, Function<Integer, PhoenixHelper.Column> toColumn) {
        if (null == cohortArr || cohortArr.length == 0) {
            return Collections.emptyMap();
        }
        Arrays.stream(cohortArr).filter(c -> !cohortMap.containsKey(c))
                .forEach(c -> {throw new IllegalStateException("Cohort name not known: " + c);});
        return Arrays.stream(cohortArr)
                        .map(c -> toColumn.apply(cohortMap.get(c)))
                        .collect(Collectors.toMap(c -> c, c -> cutoff));
    }

    private void setupMafFilters(Configuration conf, StudyConfiguration sc) {
        int studyId = sc.getStudyId();
        BiMap<String, Integer> cohortMap = sc.getCohortIds();
        float defCutoff = conf.getFloat(CONFIG_ANALYSIS_EXPORT_MAF_VALUE, 0.0F);
        Map<PhoenixHelper.Column, Float> defMap = buildFilterMap(
                conf.getStrings(CONFIG_ANALYSIS_EXPORT_MAF_COHORTS),
                defCutoff,
                cohortMap,
                cid -> VariantPhoenixHelper.getMafColumn(studyId, cid));
        if (!defMap.isEmpty()) {
            chromMafFilters.put(DEFAULT_CHROM, defMap);
        }
        getLog().info("Using {} cohorts to filter on MAF {}",
                chromMafFilters.getOrDefault(DEFAULT_CHROM, Collections.emptyMap()).keySet().stream().map(c -> c.column()).collect(Collectors.toList()),
                defCutoff);
    }

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

        this.oprFilter = new ExportOprFilter(
                getHelper().getColumnFamily(),
                (v) -> getHelper().extractVariantFromVariantRowKey(v.getRow()).getChromosome());
        this.oprFilter.configure(getStudyConfiguration(), context.getConfiguration());

        setupMafFilters(context.getConfiguration(), sc);

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
        return isValidOpr(value, variant) && isValidMaf(value, variant);
    }

    protected boolean isValidOpr(Result value, Variant variant) {
        return this.oprFilter.isValidOpr(value, variant);
    }

    protected boolean isValidMaf(Result value, Variant variant) {
        if (this.chromMafFilters.isEmpty()) {
            return true;
        }
        Map<PhoenixHelper.Column, Float> filterMap = this.chromMafFilters.get(DEFAULT_CHROM);
        for (Map.Entry<PhoenixHelper.Column, Float> entry : filterMap.entrySet()) {
            Float fValue = extractFloat(value, entry.getKey());
            if (null != fValue) {
                if (fValue > entry.getValue()) {
                    return true;
                }
            }
        }
        return false;
    }

    private Float extractFloat(Result value, PhoenixHelper.Column field) {
        Cell cell = value.getColumnLatestCell(getHelper().getColumnFamily(), field.bytes());
        byte[] bytes = CellUtil.cloneValue(cell);
        if (bytes.length == 0) {
            return null;
        }
        return (Float) PFloat.INSTANCE.toObject(bytes);
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
