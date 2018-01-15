package vilmaa.genome.storage.hbase.allele.count;

import vilmaa.genome.storage.hbase.VariantHbaseUtil;
import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator.DEL_SYMBOL;
import static vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator.INS_SYMBOL;

/**
 * Created by mh719 on 25/01/2017.
 */
public class AlleleCountToHBaseConverter {
    private static final Set<String> BASES = new HashSet<>(Arrays.asList("A", "T", "G", "C"));
    public static final String REFERENCE_PREFIX = Character.toString(HBaseToAlleleCountConverter.REFERENCE_PREFIX_CHAR);
    public static final String FILTER_FAIL = Character.toString(HBaseToAlleleCountConverter.FILTER_FAIL_CHAR);
    public static final String FILTER_PASS = Character.toString(HBaseToAlleleCountConverter.FILTER_PASS_CHAR);
    public static final String VARIANT_PREFIX = Character.toString(HBaseToAlleleCountConverter.VARIANT_PREFIX_CHAR);

    private final String studyId;
    private final byte[] family;

    public AlleleCountToHBaseConverter(byte[] family, String studyId) {
        this.studyId = studyId;
        this.family = family;
    }

    public Put convertPut(String chromosome, Integer position, String ref, String alt, AlleleCountPosition count) {
        Put put = new Put(buildRowKey(chromosome, position, ref, alt));
        boolean doSend = convertVariant(count, (col, val) -> put.addColumn(getFamily(), col, val));
        boolean refVal = convertReference(count, (col, val) -> put.addColumn(getFamily(), col, val));
        if (!doSend) {
            throw new IllegalStateException("Variant has no data to send: "
                    + chromosome + ":" + position + ":" + ref + ":" + alt);
        }
        VariantType type = VariantHbaseUtil.inferType(ref, alt);
        put.addColumn(
                getFamily(),
                VariantPhoenixHelper.VariantColumn.TYPE.bytes(),
                Bytes.toBytes(type.toString()));
        return put;
    }

    public Append convert(String chromosome, Integer position, String variantId, AlleleCountPosition count) {
        String[] split = splitVariantId(variantId);
        Append append = new Append(buildRowKey(chromosome, position, split[0], split[1]));
        boolean doSend = convertVariant(count, (col, val) -> append.add(getFamily(), col, val));
        if (!doSend) {
            throw new IllegalStateException("No data found for Variant " + variantId);
        }
        return append;
    }

    public boolean convertVariant(AlleleCountPosition count, BiConsumer<byte[], byte[]> consumer) {
        AtomicBoolean hasChanges = new AtomicBoolean(false);
        count.getAlternate().forEach((allele, samples) -> {
            consumer.accept(buildQualifier(VARIANT_PREFIX, allele), buildValue(samples));
            hasChanges.set(true);
        });
        return hasChanges.get();
    }

    protected static String[] splitVariantId(String variantId) {
        String[] split = variantId.split("_", 2);
        if (split.length != 2) {
            throw new IllegalStateException("Not able to split variant id: " + variantId);
        }
        return split;
    }

    public Append convert(String chromosome, Integer position, AlleleCountPosition countPosition) {
        Append append = new Append(buildRowKey(chromosome, position));
        boolean doSend = convertReference(countPosition, (col, val) -> append.add(getFamily(), col, val));
        if (!doSend) {
            return null;
        }
        return append;
    }

    public boolean convertReference(AlleleCountPosition countPosition, BiConsumer<byte[], byte[]> consumer) {
        boolean send = false;

        // REF data
        Map<Integer, List<Integer>> ref = countPosition.getReference();
        for (Map.Entry<Integer, List<Integer>> entry : ref.entrySet()) {
            if (entry.getKey().equals(2)) {
                continue; // Exclude HOM_REF
            }
            if (entry.getValue().isEmpty()) {
                continue; // skip empty values!!!
            }
            send = true;
            consumer.accept(buildQualifier(REFERENCE_PREFIX, entry.getKey()), buildValue(entry.getValue()));
        }
        // FAIL data
        if (!countPosition.getNotPass().isEmpty()) {
            send = true;
            consumer.accept(buildQualifier(FILTER_FAIL, ""), buildValue(countPosition.getNotPass()));
        }

        Map<String, Map<Integer, List<Integer>>> alts = countPosition.getAltMap();
        for (Map.Entry<String, Map<Integer, List<Integer>>> entry : alts.entrySet()) {
            String variantId = entry.getKey();
            for (Map.Entry<Integer, List<Integer>> alleleEntry : entry.getValue().entrySet()) {
                List<Integer> samples = alleleEntry.getValue();
                if (null != samples && !samples.isEmpty()) {
                    if (variantId.equals(DEL_SYMBOL) || variantId.equals(INS_SYMBOL) || BASES.contains(variantId)) {
                        send = true;
                        consumer.accept(buildQualifier(VARIANT_PREFIX, variantId, alleleEntry.getKey()), buildValue(samples));
                    } else {
                        throw new IllegalStateException("Unexpected variant in Reference row: " + variantId);
                    }
                }
            }
        }
        return send;
    }

    private byte[] buildRowKey(String chr, Integer position) {
        return buildRowKey(chr, position, StringUtils.EMPTY, Allele.NO_CALL_STRING);
    }

    public static byte[] buildRowKey(String chromosome, Integer position, String ref, String alt) {
        return GenomeHelper.generateVariantRowKey(chromosome, position, ref, alt);

    }

    public static byte[] buildQualifier(String r, Integer key) {
        return Bytes.toBytes(buildQualifierName(r, key));
    }

    public static String buildQualifierName(String r, Integer key) {
        return r + key;
    }

    public static  byte[] buildQualifier(String r, String key) {
        return Bytes.toBytes(r + key);
    }
    public static  byte[] buildQualifier(String r, String key, Integer allele) {
        return Bytes.toBytes(r + key + "_" + allele);
    }

    public static byte[] buildValue(Collection<Integer> value) {
        if (value.isEmpty()) {
            throw new IllegalStateException("Collection is empty!!!");
        }
        List<Integer> lst = new ArrayList<>(new HashSet<>(value));
        Collections.sort(lst);
        return PhoenixHelper.toBytes(lst, PUnsignedIntArray.INSTANCE);
    }

    public byte[] getFamily() {
        return family;
    }

}
