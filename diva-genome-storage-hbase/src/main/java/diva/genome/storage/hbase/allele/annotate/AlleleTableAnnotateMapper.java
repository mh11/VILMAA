package diva.genome.storage.hbase.allele.annotate;

import diva.genome.storage.hbase.VariantHbaseUtil;
import diva.genome.storage.hbase.filter.ExportFilters;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static diva.genome.storage.hbase.allele.annotate.AlleleTableAnnotateDriver.CONFIG_ANNOTATE_BATCH;
import static diva.genome.storage.hbase.allele.annotate.AlleleTableAnnotateDriver.CONFIG_ANNOTATE_FORCE;
import static org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.ANNOTATION_SOURCE;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.ANNOTATOR_CELLBASE_EXCLUDE;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.ANNOTATOR_CELLBASE_INCLUDE;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION;

/**
 * Created by mh719 on 05/01/2018.
 */
public class AlleleTableAnnotateMapper extends AbstractHBaseMapReduce<ImmutableBytesWritable, Put> {


    private ExportFilters filters;
    private boolean forceAnnotation;
    private byte[] studiesRow;
    private HBaseToVariantAnnotationConverter hBaseToVariantAnnotationConverter;
    private VariantAnnotator variantAnnotator;

    private final CopyOnWriteArrayList<Variant> variantsToAnnotate = new CopyOnWriteArrayList<>();
    private int batchSize = 200;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        StudyConfiguration sc = getStudyConfiguration();
        this.filters = ExportFilters.build(sc, context.getConfiguration(), getHelper().getColumnFamily());
        this.forceAnnotation = context.getConfiguration().getBoolean(CONFIG_ANNOTATE_FORCE, false);
        this.batchSize = context.getConfiguration().getInt(CONFIG_ANNOTATE_BATCH, 200);
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        this.hBaseToVariantAnnotationConverter = new HBaseToVariantAnnotationConverter(getHelper());

        /* Annotator config */
        String configFile = "storage-configuration.yml";
        String storageEngine = "hadoop"; //
        ObjectMap options = new ObjectMap(); // empty
        if (!Objects.isNull(context.getConfiguration().get(ANNOTATOR_CELLBASE_EXCLUDE, null))) {
            options.put(ANNOTATOR_CELLBASE_EXCLUDE, context.getConfiguration().get(ANNOTATOR_CELLBASE_EXCLUDE));
        }
        if (!Objects.isNull(context.getConfiguration().get(ANNOTATOR_CELLBASE_INCLUDE, null))) {
            options.put(ANNOTATOR_CELLBASE_INCLUDE, context.getConfiguration().get(ANNOTATOR_CELLBASE_INCLUDE));
        }
        if (!Objects.isNull(context.getConfiguration().get(ANNOTATION_SOURCE, null))) {
            options.put(ANNOTATION_SOURCE, context.getConfiguration().get(ANNOTATION_SOURCE));
        }
        try {
            StorageConfiguration storageConfiguration = StorageConfiguration.load(
                    StorageConfiguration.class.getClassLoader().getResourceAsStream(configFile));
            this.variantAnnotator = VariantAnnotatorFactory.buildVariantAnnotator(storageConfiguration, storageEngine, options);
        } catch (Exception e) {
            throw new IllegalStateException("Problems loading storage configuration from " + configFile, e);
        }
    }

    protected void annotateVariants(Context context, boolean force) throws IOException, InterruptedException, VariantAnnotatorException {
        if (this.variantsToAnnotate.isEmpty()) {
            return;
        }
        // not enough data
        if (this.variantsToAnnotate.size() < this.batchSize && !force) {
            return;
        }
        getLog().info("Submit {} variants ... ", this.variantsToAnnotate.size());
        long start = System.nanoTime();
        List<VariantAnnotation> annotate = this.variantAnnotator.annotate(this.variantsToAnnotate);
        getLog().info("Received {} [annot time: {}] ... ", annotate.size(), System.nanoTime() - start);
        start = System.nanoTime();
        for (VariantAnnotation annotation : annotate) {
            String fullAnnot = annotation.toString();
            byte[] rowKey = GenomeHelper.generateVariantRowKey(
                    annotation.getChromosome(),
                    annotation.getStart(),
                    annotation.getReference(),
                    annotation.getAlternate());
            Put put = new Put(rowKey);
            put.addColumn(getHelper().getColumnFamily(), FULL_ANNOTATION.bytes(), Bytes.toBytes(fullAnnot));
            context.getCounter("diva", "variant.annotate.submit").increment(1);
            context.write(new ImmutableBytesWritable(rowKey), put);
        }
        getLog().info("Done [submit time: {}] ... ", System.nanoTime() - start);
        this.variantsToAnnotate.clear();
    }


    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);
        try {
            while (context.nextKeyValue()) {
                this.map(context.getCurrentKey(), context.getCurrentValue(), context);
                annotateVariants(context, false);
            }
            annotateVariants(context, true);
        } catch (VariantAnnotatorException e) {
            throw new RuntimeException(e);
        } finally {
            this.cleanup(context);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String hexBytes = Bytes.toHex(key.get());
        Cell[] cells = value.rawCells();
        if (isMetaRow(key, value)) {
            return; // ignore META row
        }
        try {
            if (cells.length < 2) {
                context.getCounter("diva", "row.empty").increment(1);
                return;
            }
            if (!Bytes.startsWith(value.getRow(), this.studiesRow)) { // ignore _METADATA row
                context.getCounter("diva", "variant.read").increment(1);
                getLog().info("Convert ... ");
                long start = System.nanoTime();
                Variant variant = VariantHbaseUtil.inferAndSetType(
                        getHelper().extractVariantFromVariantRowKey(value.getRow()));
                if (!validVariant(value, variant)) {
                    context.getCounter("diva", "filter.remove").increment(1);
                    return;
                }

                VariantAnnotation annotation = convertAnnotations(value);
                if (!requireAnnotation(annotation)) {
                    context.getCounter("diva", "variant.no-annotation-required").increment(1);
                    return; // No annotation needed
                }
                context.getCounter("diva", "variant.add-for-annot").increment(1);
                variantsToAnnotate.add(variant);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Problems with row [hex:" + hexBytes + "] for cells " + cells.length, e);
        }
        return;

    }

    protected boolean requireAnnotation(VariantAnnotation annotation) {
        if (this.forceAnnotation) {
            return true;
        }
        if (annotation == null) {
            return true;
        }
        // Chromosome not set -> require annotation !!!!
        return StringUtils.isEmpty(annotation.getChromosome());
    }

    protected VariantAnnotation convertAnnotations(Result value) {
        return this.hBaseToVariantAnnotationConverter.convert(value);
    }

    private boolean validVariant(Result value, Variant variant) {
        return filters.pass(value, variant);
    }

    private boolean isMetaRow(ImmutableBytesWritable key, Result value) {
        return Bytes.startsWith(this.studiesRow, key.get());
    }
}
