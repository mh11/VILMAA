package diva.genome.storage.hbase.allele.exporter;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.AbstractLocalRunner;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantVcfDataWriter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;


/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleTableToVariantRunner extends AbstractLocalRunner {
    public static final String OUTPUT_VCF_FILE = "diva.allele.output.vcf";
    private HBaseAlleleCountsToVariantConverter variantConverter;
    private VariantVcfDataWriter vcfDataWriter;

    @Override
    protected void map(Scan scan, String variantTable) {
        try {
            prepareVcf(() -> super.map(scan, variantTable));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void prepareVcf(Runnable runnable) throws IOException {
        File outVCF = getOutputFile();
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(getStudyConfiguration());
        variantConverter = new HBaseAlleleCountsToVariantConverter(getHelper(), getStudyConfiguration());
        variantConverter.setReturnSamples(indexedSamples.keySet());
        variantConverter.setStudyNameAsStudyId(true);
        QueryOptions options = new QueryOptions();
        VariantSourceDBAdaptor source = new HadoopVariantSourceDBAdaptor(getHelper());

        try (OutputStream out = new FileOutputStream(outVCF)) {
            vcfDataWriter = new VariantVcfDataWriter(getStudyConfiguration(), source, out, options);
            vcfDataWriter.setExportGenotype(true);
            vcfDataWriter.open();
            vcfDataWriter.pre();
            // do the work
            runnable.run();
            // clean up
            vcfDataWriter.post();
            vcfDataWriter.close();
        } catch (Exception e) {
            getLog().error("Problems with VCF conversion", e);
            throw new IllegalStateException(e);
        }
    }

    private File getOutputFile() {
        String outVcf = getConf().get(OUTPUT_VCF_FILE, StringUtils.EMPTY);
        if (StringUtils.isBlank(outVcf)) {
            throw new IllegalStateException("VCF output paramter required: " + OUTPUT_VCF_FILE);
        }
        File outFile = new File(outVcf);
        if (outFile.exists()) {
            throw new IllegalStateException("VCF output already exists !!!");
        }
        if (!outFile.getParentFile().exists()) {
            throw new IllegalStateException("VCF output directory does not exist !!!" + outFile.getParentFile());
        }
        return outFile;
    }

    @Override
    protected void map(Result result) throws IOException {
        writeVcf(result);
    }

    protected void writeVcf(Result result) {
        Variant variant = variantConverter.convert(result);
        if (getLog().isDebugEnabled()) {
            AlleleCountPosition convert = variantConverter.getAlleleCountConverter().convert(result);
            getLog().debug("Convert {} from \n{} ", variant, convert.toDebugString());
        }
        vcfDataWriter.write(Collections.singletonList(variant));
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, new AlleleTableToVariantRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
