package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.stats.VariantTypeSummaryMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;

/**
 * Created by mh719 on 12/02/2017.
 */
public class VariantTypeSummaryDriver extends AbstractAnalysisTableDriver {

    public VariantTypeSummaryDriver() { /* nothing */ }

    public VariantTypeSummaryDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {

    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTypeSummaryMapper.class;
    }


    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTypeSummaryDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
