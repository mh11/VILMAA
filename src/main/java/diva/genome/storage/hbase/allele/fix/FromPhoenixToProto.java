package diva.genome.storage.hbase.allele.fix;

import diva.genome.storage.hbase.allele.AlleleCalculatorDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mh719 on 09/02/2017.
 */
public class FromPhoenixToProto extends AlleleCalculatorDriver {
    protected static final Logger LOG = LoggerFactory.getLogger(FromPhoenixToProto.class);
    public static final String JOB_OPERATION_NAME = "Move Alleles";

    public FromPhoenixToProto() { /* nothing */ }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return FromPhoenixToProtoMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new FromPhoenixToProto()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf, FromPhoenixToProto driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }


}
