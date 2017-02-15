package diva.genome.storage.hbase.allele.opencga;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.io.IOException;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HBaseVariantDBAdaptor extends VariantHadoopDBAdaptor {

    private final HBaseVariantSourceDBAdaptor hBaseVariantSourceDBAdaptor;

    public HBaseVariantDBAdaptor(HBaseCredentials credentials, StorageConfiguration configuration, Configuration conf) throws IOException {
        this(null, credentials, configuration, getHbaseConfiguration(conf, credentials));
    }

    public HBaseVariantDBAdaptor(Connection connection, HBaseCredentials credentials, StorageConfiguration
            configuration, Configuration conf) throws IOException {
        super(connection, credentials, configuration, conf);
        this.hBaseVariantSourceDBAdaptor = new HBaseVariantSourceDBAdaptor(getGenomeHelper());
    }

    @Override
    public HadoopVariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return this.hBaseVariantSourceDBAdaptor;
    }
}
