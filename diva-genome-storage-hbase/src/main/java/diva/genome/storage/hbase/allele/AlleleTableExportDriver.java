package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.exporter.AlleleTableToVariantMapper;
import diva.genome.storage.hbase.allele.exporter.HadoopVcfDivaOutputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.hadoop.variant.exporters.VariantTableExportDriver;

import java.io.IOException;

/**
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableExportDriver extends VariantTableExportDriver {

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AlleleTableToVariantMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);

        FileOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
        switch (this.type) {
            case AVRO:
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema()); // Set schema
                break;
            case VCF:
                job.setOutputFormatClass(HadoopVcfDivaOutputFormat.class);
                break;
            default:
                throw new IllegalStateException("Type not known: " + this.type);
        }
        job.setNumReduceTasks(0);
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
