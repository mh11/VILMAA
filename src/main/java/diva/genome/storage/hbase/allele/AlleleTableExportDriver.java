package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.exporter.AlleleTableToVariantMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.opencb.opencga.storage.hadoop.variant.exporters.VariantTableExportDriver;

/**
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableExportDriver extends VariantTableExportDriver {

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AlleleTableToVariantMapper.class;
    }


    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AlleleTableExportDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
