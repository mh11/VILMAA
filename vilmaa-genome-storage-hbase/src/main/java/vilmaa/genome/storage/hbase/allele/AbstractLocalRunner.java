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

import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver.OPENCGA_ANALYSIS_REGION;
import static org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver.parseRegion;
import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.CONFIG_VARIANT_FILE_IDS;
import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.CONFIG_VARIANT_TABLE_NAME;

/**
 * Created by mh719 on 08/02/2017.
 */
public abstract class AbstractLocalRunner extends Configured {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration;

    public VariantTableHelper getHelper() {
        return helper;
    }

    public void setHelper(VariantTableHelper helper) {
        this.helper = helper;
    }

    public StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

    public void run() throws IOException {
        String archiveTable = getConf().get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
        String variantTable = getConf().get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
        Integer studyId = getConf().getInt(GenomeHelper.CONFIG_STUDY_ID, -1);

        GenomeHelper.setStudyId(getConf(), studyId);
        VariantTableHelper.setOutputTableName(getConf(), variantTable);
        VariantTableHelper.setInputTableName(getConf(), archiveTable);

        helper = new VariantTableHelper(getConf());
        studyConfiguration = helper.loadMeta();

        Scan scan = createScan();
        map(scan, variantTable);
    }

    protected void map(Scan scan, String variantTable) {
        try {
            helper.getHBaseManager().act(variantTable, c -> {
                map(c.getScanner(scan));
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void map(ResultScanner scanner) throws IOException {
        for (Result result : scanner) {
            map(result);
        }
    }

    protected abstract void map(Result result) throws IOException;


    protected Scan createScan() {
        Scan scan = new Scan();
//        int caching = getConf().getInt(AbstractAnalysisTableDriver.HBASE_SCAN_CACHING, 50);
//        getLog().info("Scan set Caching to " + caching);
//        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(helper.getColumnFamily()); // Ignore PHOENIX columns!!!
        /* Filter for a specific region */
        String region = getConf().get(OPENCGA_ANALYSIS_REGION, StringUtils.EMPTY);
        if (StringUtils.isNotBlank(region)) {
            Pair<String, Pair<Integer, Integer>> pair = parseRegion(region);
            String chrom = pair.getLeft();
            Integer start = pair.getRight().getLeft();
            Integer end = pair.getRight().getRight();
            getLog().info("Restrict scan for region {}:{}-{}", chrom, start, end);
            /* start */
            scan.setStartRow(generateRowKey(chrom, start));
            /* end */
            scan.setStopRow(generateRowKey(chrom, end));
        }
        return scan;
    }

    protected byte[] generateRowKey(String chrom, Integer position) {
        return GenomeHelper.generateVariantRowKey(
                chrom, position, StringUtils.EMPTY, Allele.NO_CALL_STRING);
    }

    protected void configFromArgs(String[] args, int fixedSizeArgs) {
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }
    }

    public Logger getLog() {
        return LOG;
    }


    protected static int privateMain(String[] args, AbstractLocalRunner runner) throws IOException {
        runner.setConf(HBaseConfiguration.create());
        runner.configFromArgs(args, 5);
        runner.run();
        return 0;
    }

}
