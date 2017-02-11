package diva.genome.storage.hbase.allele.transfer;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import diva.genome.storage.hbase.allele.exporter.AlleleTableToVariantRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.OpencgaMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;


/**
 * Created by mh719 on 01/02/2017.
 */
public class HBaseTransferAlleleRunner extends AlleleTableToVariantRunner {

    @Override
    protected void map(Scan scan, String variantTable) {

        List<Put> putList = new ArrayList<>();
        try {
            MyMapper mapper = buildMapper(getHelper(), getStudyConfiguration());
            HBaseManager.HBaseTableConsumer consumer =
                    c -> mapper.runAlternative(c.getScanner(scan), put -> putList.add(put));
            getHelper().getHBaseManager().act(variantTable, consumer);
            prepareVcf(() -> {
                putList.forEach(p -> writeVcf(Result.create(p.getFamilyCellMap().get(getHelper().getColumnFamily()))));
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected MyMapper buildMapper(VariantTableHelper gh, StudyConfiguration studyConfiguration) throws IOException {
        MyMapper mapper = new MyMapper();
        OpencgaMapReduceHelper mrHelper = new OpencgaMapReduceHelper(null);
        mrHelper.setHelper(gh);
        mrHelper.setStudyConfiguration(studyConfiguration);
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
        mrHelper.setIndexedSamples(indexedSamples);
        mrHelper.setTimestamp(1);

        mapper.setMrHelper(mrHelper);
        mapper.alleleCountConverter = new HBaseToAlleleCountConverter();
        mapper.setStudiesRow(gh.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0));
        mapper.alleleCombiner = new AlleleCombiner(new HashSet<>(indexedSamples.values()));
        mapper.converter = new AlleleCountToHBaseConverter(gh.getColumnFamily(), gh.getStudyId() + "");
        return mapper;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, new HBaseTransferAlleleRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static class MyMapper extends HbaseTransferAlleleMapper {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // nothing
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // nothing
        }

        @Override
        protected Put newTransfer(Variant variant, AlleleCountPosition from, AlleleCountPosition to) {
            Put put = super.newTransfer(variant, from, to);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Merged from: \n{}" , from.toDebugString());
                getLog().debug("Merged {} into: \n{}" , variant, to.toDebugString());
            }
            return put;
        }

        public void runAlternative(ResultScanner scanner, Consumer<Put> submitFunction) {
            try {
                // buffer
                String chromosome = "-1";
                Integer referencePosition = -1;
                Result referenceResult = null;
                AlleleCountPosition refBean = null;
                clearRegionOverlap();
                for (Result result : scanner) {
                    if (isMetaRow(result.getRow())) {
                        continue;
                    }
                    Variant variant = getHelper().extractVariantFromVariantRowKey(result.getRow());
                    if (variant.getStart() > referencePosition && !positionBuffer.isEmpty()) {
                        getLog().info("Process buffer of {} for position ... ", positionBuffer.size(), referencePosition);

                        processBuffer(refBean, submitFunction);
                        positionBuffer.clear();
                    }
                    if (!StringUtils.equals(chromosome, variant.getChromosome())) {
                        referencePosition = -1;
                        clearRegionOverlap();
                        chromosome = variant.getChromosome();
                    }
                    checkDeletionOverlapMap(variant.getStart());
                    if (variant.getType().equals(VariantType.NO_VARIATION)) {
                        referencePosition = variant.getStart();
                        referenceResult = result;
                        refBean = null;
                        continue;
                    }
                    // if actual variant
                    if (null == referencePosition || !referencePosition.equals(variant.getStart())) {
                        // should only happen at the start of a split block.
                        referenceResult = queryForRefernce(variant);
                    }
                    if (refBean == null) {
                        refBean = this.alleleCountConverter.convert(referenceResult);
                    }
                    this.positionBuffer.add(new ImmutablePair<>(result, variant));
                }
                getLog().info("Clear buffer ...");
                if (!positionBuffer.isEmpty()) {
                    processBuffer(refBean, submitFunction);
                    positionBuffer.clear();
                }
                getLog().info("Done ...");
            } catch (Exception e) {
                throw new IllegalStateException("Something went wrong during transfer", e);
            }
        }

        @Override
        public void run(Context ctx) throws IOException, InterruptedException {
           // do nothing
        }

    }

}
