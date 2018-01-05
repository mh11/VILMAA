package diva.genome.storage.hbase.allele.annotate;

import diva.genome.storage.hbase.allele.AbstractAlleleDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;

/**
 * Execute MapReduce job to annotate variants.
 * Created by mh719 on 05/01/2018.
 */
public class AlleleTableAnnotateDriver extends AbstractAlleleDriver {
    public static final String CONFIG_ANNOTATE_FORCE = "diva.genome.storage.hbase.allele.annotate.force";
    public static final String CONFIG_ANNOTATE_BATCH = "diva.genome.storage.hbase.allele.annotate.batchsize";
    public static final String CONFIG_ANNOTATE_PARALLEL = "diva.genome.storage.hbase.allele.annotate.parallel";

    public AlleleTableAnnotateDriver() {
        super();
    }

    public AlleleTableAnnotateDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        int parallel = getConf().getInt(CONFIG_ANNOTATE_PARALLEL, 5);
        getConf().setInt("mapreduce.job.running.map.limit", parallel);
        getConf().setLong("phoenix.upsert.batch.size", 200L);
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AlleleTableAnnotateMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                inTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        super.preExecution(variantTable);
        // no need to index with Phoenix!!!
    }

    public static void main(String[] args) {
        try {
            System.exit(privateMain(args, null, new AlleleTableAnnotateDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
