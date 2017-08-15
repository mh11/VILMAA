package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import diva.genome.storage.hbase.allele.AbstractAlleleDriver;
import diva.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.exception.OutOfRangeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GenomeAnalysisDriver extends AbstractAlleleDriver {
    public static final String CONFIG_ANALYSIS_EXPORT_AVRO_PATH = "diva.genome.analysis.models.avro.file";
    public static final String CONFIG_ANALYSIS_MR_ANALYSISTYPE = "diva.genome.analysis.mr.analysis.type";
    public static final String CONFIG_ANALYSIS_ASSOC_CASES = "diva.genome.analysis.association.cases";
    public static final String CONFIG_ANALYSIS_ASSOC_CTL = "diva.genome.analysis.association.controls";
    public static final String CONFIG_ANALYSIS_PREFILTER_CTL_MAF = "diva.genome.analysis.prefilter.ctrl.maf";
    public static final String CONFIG_ANALYSIS_FILTER_POP_AF = "diva.genome.analysis.filter.pop.af";
    public static final String CONFIG_ANALYSIS_FILTER_CTL_MAF_AUTO = "diva.genome.analysis.filter.ctrl.maf.auto";
    public static final String CONFIG_ANALYSIS_FILTER_CTL_MAF_X = "diva.genome.analysis.filter.ctrl.maf.X";
    public static final String CONFIG_ANALYSIS_FILTER_OPR =              "diva.genome.analysis.filter.opr";
    public static final String CONFIG_ANALYSIS_FILTER_OPR_COHORTS =      "diva.genome.analysis.filter.opr.cohorts";
    public static final String CONFIG_ANALYSIS_FILTER_OPR_X =            "diva.genome.analysis.filter.opr.x";
    public static final String CONFIG_ANALYSIS_FILTER_OPR_X_COHORTS =    "diva.genome.analysis.filter.opr.x.cohorts";
    public static final String CONFIG_ANALYSIS_FILTER_OPR_Y =            "diva.genome.analysis.filter.opr.y";
    public static final String CONFIG_ANALYSIS_FILTER_OPR_Y_COHORTS =    "diva.genome.analysis.filter.opr.y.cohorts";
    public static final String CONFIG_ANALYSIS_FILTER_COMBINED_CADD =    "diva.genome.analysis.analysis.combined.cadd";

    private Path outAvroFile;

    public GenomeAnalysisDriver() { /* nothing */ }

    public GenomeAnalysisDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        super.parseAndValidateParameters();
        String file = null;
        if (!Objects.isNull(getConf().get(CONFIG_ANALYSIS_EXPORT_AVRO_PATH, null))) {
            file = getConf().get(CONFIG_ANALYSIS_EXPORT_AVRO_PATH);
        }
        if (StringUtils.isBlank(file)) {
            throw new IllegalArgumentException("No avro output path specified using " + CONFIG_ANALYSIS_EXPORT_AVRO_PATH);
        }
        assertConfigExist(CONFIG_ANALYSIS_ASSOC_CASES);
        assertConfigExist(CONFIG_ANALYSIS_ASSOC_CTL);
        assertConfigExist(CONFIG_ANALYSIS_FILTER_CTL_MAF_AUTO);
        assertConfigExist(CONFIG_ANALYSIS_FILTER_CTL_MAF_X);
        assertConfigExist(CONFIG_ANALYSIS_MR_ANALYSISTYPE);
        assertConfigExist(CONFIG_ANALYSIS_FILTER_OPR);
        assertConfigExist(CONFIG_ANALYSIS_FILTER_POP_AF);
        outAvroFile = new Path(file);
    }

    private void assertConfigExist(String prop) {
        if (StringUtils.isBlank(getConf().get(prop))) {
            throw new IllegalStateException("Property expected: " + prop);
        }
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return GeneSummaryMapper.class;
    }

    private Class<? extends Reducer> getCombinerClass() {
        return GeneSummaryCombiner.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String analysisTable = getHelper().getOutputTableAsString();
        getLog().info("Read from {} table ...", analysisTable);
        super.initMapReduceJob(analysisTable, job, scan, addDependencyJar);

        job.setCombinerClass(getCombinerClass());

        getLog().info("Write to {} ouptut file ...", this.outAvroFile);

        FileOutputFormat.setOutputPath(job, this.outAvroFile); // set Path
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); // compression

        job.setReducerClass(GeneSummaryReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);

        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, GeneSummary.getClassSchema()); // Set schema

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    }

    @Override
    protected Scan createScan() {
        String ctlCohort = getConf().get(CONFIG_ANALYSIS_ASSOC_CTL);
        float mafCutoff = getConf().getFloat(CONFIG_ANALYSIS_PREFILTER_CTL_MAF,
                Math.max(getConf().getFloat(CONFIG_ANALYSIS_FILTER_CTL_MAF_AUTO, -1),
                        getConf().getFloat(CONFIG_ANALYSIS_FILTER_CTL_MAF_X, -1)));
        if (mafCutoff < 0 || mafCutoff > 1) {
            throw new OutOfRangeException(mafCutoff, 0, 1);
        }
        int studyId = getHelper().getStudyId();
        try {
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            StudyConfiguration sc = getHelper().loadMeta();
            Integer cohortId = sc.getCohortIds().get(ctlCohort);
            /* MAF filter */
            byte[] mafColumn = VariantPhoenixHelper.getMafColumn(studyId, cohortId).bytes();
            SingleColumnValueFilter mafCtlFilter = new SingleColumnValueFilter(
                    getHelper().getColumnFamily(),
                    mafColumn,
                    CompareFilter.CompareOp.LESS,
                    PFloat.INSTANCE.toBytes(mafCutoff));
            mafCtlFilter.setFilterIfMissing(false);
            mafCtlFilter.setLatestVersionOnly(true);
            getLog().info("Register MAF filter {} on column {} ", mafCutoff, Bytes.toString(mafColumn));

            filterList.addFilter(mafCtlFilter);

            Scan scan = super.createScan();
            scan.setFilter(filterList);
            return scan;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class GeneSummaryReducer extends Reducer<Text, ImmutableBytesWritable, AvroKey<CharSequence>, AvroValue<GeneSummary>> {

        private GeneSummaryCombiner geneSummaryCombiner;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            geneSummaryCombiner = new GeneSummaryCombiner();
            geneSummaryCombiner.init();
        }

        @Override
        protected void reduce(Text key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
            context.getCounter("DIVA", "reduce").increment(1);
            context.write(new AvroKey<>(key.toString()), new AvroValue<>(geneSummaryCombiner.combine(values)));
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new GenomeAnalysisDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
