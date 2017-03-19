package diva.genome.storage.hbase.allele.count;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.Arrays;

/**
 * Bean to hold sample specific allele information
 * Created by mh719 on 17/03/2017.
 */
public class AlleleInfo {

    private boolean pass;
    private int count;
    private int depth;
    private VariantType type;
    private String[] id;
    private Integer sampleId;


    public AlleleInfo() {

    }

    public AlleleInfo(int count, int depth) {
        this.count = count;
        this.depth = depth;
    }

    public AlleleInfo(int count, int depth, Integer sampleId, String[] id, VariantType type, boolean pass) {
        this.pass = pass;
        this.count = count;
        this.depth = depth;
        this.type = type;
        this.id = id;
        this.sampleId = sampleId;
    }

    public void setSampleId(Integer sampleId) {
        this.sampleId = sampleId;
    }

    public Integer getSampleId() {
        return sampleId;
    }

    public void setPass(boolean pass) {
        this.pass = pass;
    }

    public boolean isPass() {
        return pass;
    }

    public int getCount() {
        return count;
    }

    public int getDepth() {
        return depth;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public void setType(VariantType type) {
        this.type = type;
    }

    public VariantType getType() {
        return type;
    }

    public void setId(String[] id) {
        this.id = id;
    }

    public String[] getId() {
        return id;
    }

    public String getIdString() {
        return buildVariantId(this.getId());
    }

    public static String buildVariantId(String[] refAlt) {
        return buildVariantId(StringUtils.EMPTY, refAlt);
    }

    public static String buildVariantId(String prefix, String[] refAlt) {
        if (refAlt.length == 0) {
            return StringUtils.EMPTY; // Reference
        }
        if (refAlt.length == 1) {
            return refAlt[0]; // NO_CALL
        }
        if (refAlt.length != 2) {
            throw new IllegalStateException("RefAlt array expected to be of length 2: " + Arrays.toString(refAlt));
        }
        return buildVariantId(prefix, refAlt[0], refAlt[1]);
    }

    public static String buildVariantId(String prefix, String ref, String alt) {
        return prefix + ref + "_" + alt;
    }
}
