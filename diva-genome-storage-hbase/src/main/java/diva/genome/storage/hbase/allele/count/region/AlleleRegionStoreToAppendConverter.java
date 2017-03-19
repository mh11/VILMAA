package diva.genome.storage.hbase.allele.count.region;

import com.google.protobuf.MessageLite;
import diva.genome.storage.hbase.allele.count.AlleleInfo;
import diva.genome.storage.hbase.allele.model.protobuf.AlleleRegionStore.Builder;
import diva.genome.storage.hbase.allele.model.protobuf.AlleleRegionStoreEntry;
import diva.genome.storage.hbase.allele.model.protobuf.AlleleType;
import diva.genome.util.Region;
import org.apache.hadoop.hbase.client.Append;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Converts an in-memory {@link AlleleRegionStore} to an append object for HBase.
 * Created by mh719 on 19/03/2017.
 */
public class AlleleRegionStoreToAppendConverter {
    protected static final byte[] DEFAULT_QUALIFIER = {1};
    private volatile ByteArrayOutputStream bout;
    private final byte[] columnFamily;
    private byte[] qualifier;

    public AlleleRegionStoreToAppendConverter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
        this.bout = new ByteArrayOutputStream();
        qualifier = DEFAULT_QUALIFIER;
    }

    public byte[] toBytes(MessageLite msg) {
        bout.reset();
        try {
            msg.writeDelimitedTo(bout);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Problems during convertion from Proto to byte[]", e);
        }
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public Collection<Append> convert(String chromosome, AlleleRegionStore store) {
        Builder builder = diva.genome.storage.hbase.allele.model.protobuf.AlleleRegionStore.newBuilder();
        store.getReference(r -> builder.addReference(convert(r)));
        store.getNocall(r -> builder.addNocall(convert(r)));
        store.getVariation(r -> builder.addVariation(convert(r)));
        byte[] bytes = toBytes(builder.build());
        Append append = new Append(buildRowKey(chromosome, store.getTargetRegion().getMinPosition()));
        append.add(getColumnFamily(), qualifier, bytes);
        return Collections.singleton(append);
    }

    private byte[] buildRowKey(String chromosome, Integer position) {
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(Integer.valueOf(chromosome.length())).intValue() +
                QueryConstants.SEPARATOR_BYTE_ARRAY.length + PUnsignedInt.INSTANCE.estimateByteSize(Integer.valueOf(position));
        byte[] rk = new byte[size];
        byte offset = 0;
        int var7 = offset + PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
        rk[var7++] = 0;
        PUnsignedInt.INSTANCE.toBytes(Integer.valueOf(position), rk, var7);
        return rk;
    }

    private AlleleRegionStoreEntry.Builder convert(Region<AlleleInfo> region) {
        return AlleleRegionStoreEntry.newBuilder()
                .setId(region.getData().getIdString())
                .setStart(region.getStart())
                .setEnd(region.getEnd())
                .setPass(region.getData().isPass())
                .setCount(region.getData().getCount())
                .setDepth(region.getData().getDepth())
                .setType(encodeType(region.getData().getType()))
                .setSampleid(region.getData().getSampleId());
    }

    private AlleleType encodeType(VariantType type) {
        switch (type) {
            case SNV:
            case SNP: return AlleleType.SNV;
            case MNV:
            case MNP: return AlleleType.MNV;
            case INDEL: throw new IllegalStateException("INDEL should be INSERTION or DELETION");
            case INSERTION: return AlleleType.INSERTION;
            case DELETION: return AlleleType.DELETION;
            case NO_VARIATION: return AlleleType.NO_VARIATION;
            default:
                throw new IllegalStateException("Not supported yet: " + type);
        }
    }
}
