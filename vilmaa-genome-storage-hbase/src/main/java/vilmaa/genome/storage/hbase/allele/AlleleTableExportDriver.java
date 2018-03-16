/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package vilmaa.genome.storage.hbase.allele;

import vilmaa.genome.storage.hbase.allele.exporter.AlleleTableToVariantMapper;
import vilmaa.genome.storage.hbase.allele.exporter.HadoopVcfVilmaaOutputFormat;
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
import java.util.Objects;

import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_GENOTYPE;
import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_PATH;

/**
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableExportDriver extends VariantTableExportDriver {

    @Override
    protected void parseAndValidateParameters() {
        mapConfig(CONFIG_ANALYSIS_EXPORT_GENOTYPE, CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE);
        mapConfig(CONFIG_ANALYSIS_EXPORT_PATH, CONFIG_VARIANT_TABLE_EXPORT_PATH);
        super.parseAndValidateParameters();
    }

    private void mapConfig(String from, String to) {
        String val = getConf().get(from, null);
        if (Objects.nonNull(val)) {
            getLog().info("Overwrite {} with {} from {} ", to, val, from);
            getConf().set(to, val);
        }
    }

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
                job.setOutputFormatClass(HadoopVcfVilmaaOutputFormat.class);
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
