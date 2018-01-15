package vilmaa.genome.storage.hbase.allele.opencga;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HBaseVariantSourceDBAdaptor extends HadoopVariantSourceDBAdaptor {


    public HBaseVariantSourceDBAdaptor(Configuration configuration) {
        super(configuration);
    }

    public HBaseVariantSourceDBAdaptor(Connection connection, Configuration configuration) {
        super(connection, configuration);
    }

    public HBaseVariantSourceDBAdaptor(GenomeHelper genomeHelper) {
        super(genomeHelper);
    }
}
