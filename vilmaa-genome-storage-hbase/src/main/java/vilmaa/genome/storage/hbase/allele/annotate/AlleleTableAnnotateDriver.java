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

package vilmaa.genome.storage.hbase.allele.annotate;

import vilmaa.genome.storage.hbase.allele.AbstractAlleleDriver;
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
    public static final String CONFIG_ANNOTATE_FORCE = "vilmaa.genome.storage.hbase.allele.annotate.force";
    public static final String CONFIG_ANNOTATE_BATCH = "vilmaa.genome.storage.hbase.allele.annotate.batchsize";
    public static final String CONFIG_ANNOTATE_PARALLEL = "vilmaa.genome.storage.hbase.allele.annotate.parallel";

    public AlleleTableAnnotateDriver() {
        super();
    }

    public AlleleTableAnnotateDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        super.parseAndValidateParameters();
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
