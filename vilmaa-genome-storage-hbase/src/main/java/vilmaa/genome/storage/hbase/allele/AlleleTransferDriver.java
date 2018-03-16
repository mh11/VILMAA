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

import vilmaa.genome.storage.hbase.allele.transfer.HbaseTransferAlleleMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.HBASE_SCAN_CACHING;

/**
 * Created by mh719 on 30/01/2017.
 */
public class AlleleTransferDriver extends AbstractAlleleDriver {

    public AlleleTransferDriver() { /* nothing */ }

    public AlleleTransferDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected Scan createScan() {
        Scan scan = super.createScan();
        int caching = getConf().getInt(HBASE_SCAN_CACHING, -1);
        if (caching > 1) {
            getLog().info("Scan set Caching to " + caching);
            scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        }
        return scan;
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return HbaseTransferAlleleMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String analysisTable = getHelper().getOutputTableAsString();
        getLog().info("Read from {} table ...", this.getCountTable());
        getLog().info("Write to {} table ...", analysisTable);
        super.initMapReduceJob(this.getCountTable(), job, scan, addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                analysisTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AlleleTransferDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


}
