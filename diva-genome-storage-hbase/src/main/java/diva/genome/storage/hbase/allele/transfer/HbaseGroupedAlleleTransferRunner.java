package diva.genome.storage.hbase.allele.transfer;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import diva.genome.storage.hbase.allele.count.converter.HBaseAppendGroupedToAlleleCountConverter;
import diva.genome.storage.hbase.allele.count.position.HBaseAlleleTransfer;
import diva.genome.storage.hbase.allele.exporter.AlleleTableToVariantRunner;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.OpencgaMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by mh719 on 21/02/2017.
 */
public class HbaseGroupedAlleleTransferRunner extends AlleleTableToVariantRunner {


    @Override
    protected void map(Scan scan, String variantTable) {

        List<Put> putList = new ArrayList<>();
        try {
            HbaseGroupedAlleleTransferRunner.MyMapper mapper = buildMapper(getHelper(), getStudyConfiguration());
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

    protected HbaseGroupedAlleleTransferRunner.MyMapper buildMapper(VariantTableHelper gh, StudyConfiguration studyConfiguration) throws IOException {
        HbaseGroupedAlleleTransferRunner.MyMapper mapper = new HbaseGroupedAlleleTransferRunner.MyMapper();
        OpencgaMapReduceHelper mrHelper = new OpencgaMapReduceHelper(null);
        mrHelper.setHelper(gh);
        mrHelper.setStudyConfiguration(studyConfiguration);
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
        mrHelper.setIndexedSamples(indexedSamples);
        mrHelper.setTimestamp(1);

        mapper.setMrHelper(mrHelper);
        mapper.groupedConverter = new HBaseAppendGroupedToAlleleCountConverter(gh.getColumnFamily());
        mapper.setStudiesRow(gh.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0));
        mapper.converter = new AlleleCountToHBaseConverter(gh.getColumnFamily(), gh.getStudyId() + "");
        mapper.hBaseAlleleTransfer = new HBaseAlleleTransfer(new HashSet<>(indexedSamples.values()));
        return mapper;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, new HbaseGroupedAlleleTransferRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static class MyMapper extends HbaseGroupedAlleleTransferMapper {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // nothing
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // nothing
        }

        @Override
        protected Put toPut(Variant variant, AlleleCountPosition to) {
            Put put = super.toPut(variant, to);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Merged {} into: \n{}" , variant, to.toDebugString());
            }
            return put;
        }

        public void runAlternative(ResultScanner scanner, Consumer<Put> submitFunction) {
            // buffer
            String chromosome = "-1";
            clearRegionOverlap();

            try {
                for (Result result : scanner) {
                    if (isMetaRow(result.getRow())) {
                        continue;
                    }
                    Pair<String, Integer> pair = groupedConverter.extractRegion(result.getRow());
                    if (!pair.getLeft().equals(chromosome)) {
                        hBaseAlleleTransfer.resetNewChromosome();
                        chromosome = pair.getLeft();
                        clearRegionOverlap();
                    }
                    Map<Integer, Pair<AlleleCountPosition, Map<String, AlleleCountPosition>>> regionData =
                            groupedConverter.convert(result);
                    List<Integer> positions = new ArrayList<>(regionData.keySet());
                    Collections.sort(positions); // sort positions

                    for (Integer position : positions) {
                        checkDeletionOverlapMap(position);
                        Pair<AlleleCountPosition, Map<String, AlleleCountPosition>> refAndAlts =
                                regionData.get(position);
                        List<Pair<AlleleCountPosition, Variant>> variants = extractToVariants(pair.getLeft(), position,
                                refAndAlts.getRight());
                        hBaseAlleleTransfer.process(refAndAlts.getLeft(), variants, (v, a) -> toPut(v, a));
                    }
                }
                getLog().info("Done ...");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void run(Context ctx) throws IOException, InterruptedException {
            // do nothing
        }

    }


}
