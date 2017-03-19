package diva.genome.storage.hbase.allele.count.position;

import diva.genome.storage.hbase.allele.count.AlleleCalculator;
import diva.genome.storage.hbase.allele.count.AlleleInfo;
import diva.genome.util.Region;
import diva.genome.util.RegionImpl;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;

import java.util.*;

/**
 * Calculates the allele count for a {@link Variant} per individual for each position
 * Created by mh719 on 17/03/2017.
 */
public abstract class AbstractAlleleCalculator implements AlleleCalculator {
    public static final Integer NO_CALL = -1;
    protected static final String ANNOTATION_FILTER = "FILTER";
    protected static final String DEFAULT_ANNOTATION_FILTER_VALUE = VariantMerger.DEFAULT_FILTER_VALUE;
    protected static final String PASS_VALUE = VariantMerger.PASS_VALUE;
    protected static final Integer REF_IDX = 0;
    public static final String DP_KEY = "DP";
    public static final String AD_KEY = "AD";
    protected static final String[] NO_CALL_ALLELE = {"."};
    protected static final String[] REFERENCE_ALLELE = new String[0];
    protected final String studyId;
    protected final Map<String, Integer> sampleNameToSampleId;
    protected final Region region;

    public AbstractAlleleCalculator(int start, int end, String studyId, Map<String, Integer> sampleNameToSampleId) {
        this(start, end, studyId, sampleNameToSampleId, false);
    }
    public AbstractAlleleCalculator(int start, int end, String studyId, Map<String, Integer> sampleNameToSampleId, boolean endInclusive) {
        this.region = new RegionImpl("x", start, endInclusive?end:end - 1);
        this.studyId = studyId;
        this.sampleNameToSampleId = sampleNameToSampleId;
    }

    public static VariantType getAlleleType(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case -1: return VariantType.NO_VARIATION;
            case 0:
            case 1: return var.getType();
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return alt.getType();
        }
    }

    public static Integer getAlleleStart(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case -1:
            case 0:
            case 1: return var.getStart();
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return alt.getStart();
        }
    }

    public static Integer getAlleleEnd(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case -1:
            case 0:
            case 1: return var.getEnd();
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return alt.getEnd();
        }
    }

    public static String[] buildRefAlt(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case -1: return NO_CALL_ALLELE;
            case 0: return REFERENCE_ALLELE;
            case 1: return new String[] { var.getReference(), var.getAlternate()};
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return new String[] { alt.getReference(), alt.getAlternate()};
        }
    }

    public static Map<Integer, Integer> getAlleleCount(String gt) {
        Map<Integer, Integer> retMap = new HashMap<>();
        int[] allelesIdx = new Genotype(gt).getAllelesIdx();
        for (Integer idx : allelesIdx) {
            Integer cnt = retMap.getOrDefault(idx, 0);
            retMap.put(idx, cnt + 1);
        }
        if (retMap.containsKey(NO_CALL) && retMap.get(NO_CALL) > 0) {
            if (retMap.size() > 1) { // other than no-call
                retMap.remove(NO_CALL);
            } else {
                retMap.put(NO_CALL, 1); // Reset to 1 Allele -> no difference between ./. and .
            }
        }
        return retMap;
    }

    protected static Map<Integer, AlleleInfo> getAlleleCount(List<String> data, Integer gtPos, Integer dpPos, Integer adPos) {
        Map<Integer, AlleleInfo> retMap = new HashMap<>();
        String gt = data.get(gtPos);
        Integer depthCount = -1;
        if (null != dpPos) {
            depthCount = Integer.valueOf(data.get(dpPos)); // not always there
        }
        List<Integer> depth = new ArrayList<>();
        if (null != adPos) {
            Arrays.stream(data.get(adPos).split(",")).forEach(s -> depth.add(Integer.valueOf(s)));
        }
        int[] allelesIdx = new Genotype(gt).getAllelesIdx();
        for (Integer idx : allelesIdx) {
            Integer ad = idx < 0 || depth.isEmpty() ? depthCount : depth.get(idx);
            AlleleInfo info = retMap.get(idx);
            if (null == info) {
                info = new AlleleInfo(1, ad);
            } else {
                info = new AlleleInfo(info.getCount() + 1, ad);
            }
            retMap.put(idx, info);
        }
        if (retMap.containsKey(NO_CALL) && retMap.get(NO_CALL).getCount() > 0) {
            if (retMap.size() > 1) { // other than no-call
                retMap.remove(NO_CALL);
            } else {
                AlleleInfo info = retMap.get(NO_CALL);
                retMap.put(NO_CALL, new AlleleInfo(1, info.getDepth())); // Reset to 1 Allele -> no difference between ./. and .
            }
        }
        return retMap;
    }

    protected Integer getSampleId(String sampleName) {
        return this.sampleNameToSampleId.get(sampleName);
    }

    protected boolean isPassFilter(StudyEntry studyEntry) {
        return StringUtils.equals(getFilterValue(studyEntry), PASS_VALUE);
    }

    private String getFilterValue(StudyEntry studyEntry) {
        if (studyEntry.getFiles().isEmpty()) {
            return DEFAULT_ANNOTATION_FILTER_VALUE;
        }
        return studyEntry.getFiles().get(0).getAttributes()
                .getOrDefault(ANNOTATION_FILTER, DEFAULT_ANNOTATION_FILTER_VALUE);
    }

//    static String buildVariantId(String[] refAlt) {
//        return buildVariantId(StringUtils.EMPTY, refAlt);
//    }
//
//    static String buildVariantId(String prefix, String[] refAlt) {
//        if (refAlt.length == 0) {
//            return StringUtils.EMPTY; // Reference
//        }
//        if (refAlt.length != 2) {
//            throw new IllegalStateException("RefAlt array expected to be of length 2: " + Arrays.toString(refAlt));
//        }
//        return buildVariantId(prefix, refAlt[0], refAlt[1]);
//    }
//
//    static String buildVariantId(String prefix, String ref, String alt) {
//        return prefix + ref + "_" + alt;
//    }
}
