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

import vilmaa.genome.storage.hbase.allele.count.GvcfToVariantMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver.OPENCGA_ANALYSIS_REGION;
import static org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver.parseRegion;

/**
 * Created by mh719 on 26/05/2017.
 */
public class GvcfToVariantDriver extends AbstractVariantTableDriver {
    protected static final Logger LOG = LoggerFactory.getLogger(GvcfToVariantDriver.class);


    public GvcfToVariantDriver() { /* nothing */ }

    private Logger getLog() {
        return LOG;
    }

    @Override
    public int run(String[] args) throws Exception {
        int fixedSizeArgs = 5;
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }

        // Set parallel pool size
        Collection<String> opts = getConf().getStringCollection("opencga.variant.table.mapreduce.map.java.opts");
        String optString = StringUtils.EMPTY;
        if (!opts.isEmpty()) {
            for (String opt : opts) {
                optString += opt + " ";
            }
        }
        if (StringUtils.isNotBlank(optString)) {
            getLog().info("Set mapreduce java opts: {}", optString);
            getConf().set("mapreduce.map.java.opts", optString);
        }
        return super.run(args);
    }

    @Override
    protected String getJobOperationName() {
        return "Merge variants";
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return GvcfToVariantMapper.class;
    }

    protected Scan createScan(VariantTableHelper gh, String[] fileArr) {
        Scan scan = new Scan();
        int caching = getConf().getInt(HBASE_SCAN_CACHING, 50);
        getLog().info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        // specify return columns (file IDs)
        for (String fileIdStr : fileArr) {
            int id = Integer.parseInt(fileIdStr);
            byte[] column = Bytes.toBytes(ArchiveHelper.getColumnName(id));
            scan.addColumn(gh.getColumnFamily(), column);
        }
        String region = getConf().get(OPENCGA_ANALYSIS_REGION, StringUtils.EMPTY);
        if (StringUtils.isNotBlank(region)) {
            Pair<String, Pair<Integer, Integer>> pair = parseRegion(region);
            String chrom = pair.getLeft();
            Integer start = pair.getRight().getLeft();
            Integer end = pair.getRight().getRight();
            getLog().info("Restrict scan for region {}:{}-{}", chrom, start, end);
            /* start */
            scan.setStartRow(getHelper().generateBlockIdAsBytes(chrom, start));
            /* end */
            scan.setStopRow(getHelper().generateBlockIdAsBytes(chrom, end));
        }
        return scan;
    }

    @Override
    protected void initMapReduceJob(String inTable, String outTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        getLog().info("Read from {} and write to {} ...", inTable, outTable);
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                getMapperClass(),   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                outTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new GvcfToVariantDriver()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf, GvcfToVariantDriver driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }

}
