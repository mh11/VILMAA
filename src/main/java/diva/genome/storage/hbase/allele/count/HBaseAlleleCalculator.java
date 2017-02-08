package diva.genome.storage.hbase.allele.count;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Calculates which alleles (and no-call) have been seen how often for an individual at each position.
 * In addition it records pass, not pass for each individual.
 *
 * Created by mh719 on 25/01/2017.
 */
public class HBaseAlleleCalculator {
    public static final String DEL_SYMBOL = "*";
    public static final String INS_SYMBOL = "+";
    public static final Integer NO_CALL = -1;
    private static final Integer REF_IDX = 0;
    protected static final String ANNOTATION_FILTER = "FILTER";
    protected static final String DEFAULT_ANNOTATION_FILTER_VALUE = VariantMerger.DEFAULT_FILTER_VALUE;
    protected static final String PASS_VALUE = VariantMerger.PASS_VALUE;

    private Logger log = LoggerFactory.getLogger(this.getClass());
    private final String studyId;
    private final Map<String, Integer> sampleNameToSampleId;

    // <position> <AlleleCnt> <SampleIds>: Allele count is -1 (nocall), 0 (other alleles), 1, 2
    private volatile Map<Integer, Map<Integer, Set<Integer>>> referenceToGtToSamples = new HashMap<>();

    // Only record  individuals with that allele
    // <Position> <VAR_ID> <Allele count> <SampleIds>: Allele count is 1,2,...
    private Map<Integer, Map<String, Map<Integer, Set<Integer>>>> variantToGtToSamples = new HashMap<>();
    private Map<Integer, Map<String, Map<Integer, Set<Integer>>>> alternateToGtToSamples = new HashMap<>();

    private final Map<Integer, Set<Integer>> passPosition = new HashMap<>();
    private final Map<Integer, Set<Integer>> notPassPosition = new HashMap<>();
    private volatile Pair<Integer, Integer> region = new ImmutablePair<>(0, Integer.MAX_VALUE);

    public HBaseAlleleCalculator(String studyId, Map<String, Integer> sampleNameToSampleId) {
        this.studyId = studyId;
        this.sampleNameToSampleId = sampleNameToSampleId;
    }

    /**
     *
     * @param start Start position (inclusive).
     * @param end End position (exclusive)
     */
    public void setRegion(int start, int end) {
        this.region = new ImmutablePair<>(start, end);
    }

    public void addVariant(Variant variant) {
        StudyEntry se = variant.getStudy(studyId);
        boolean isPass = isPassFilter(se);

        List<AlternateCoordinate> secondaryAlternates = se.getSecondaryAlternates();
        Integer gtPos = se.getFormatPositions().get(VariantMerger.GT_KEY);
        List<List<String>> samplesData = se.getSamplesData();
        se.getSamplesPosition().forEach((sampleName, sampPos) -> {
            Integer sampleId = this.getSampleId(sampleName);
            Map<Integer, Integer> alleleCount = getAlleleCount(samplesData.get(sampPos).get(gtPos));
            Integer refAlleleCnt = alleleCount.getOrDefault(REF_IDX, 0);
            updateReferenceCount(variant.getStart(), variant.getEnd(), sampleId, alleleCount);
            updatePassAnnotation(variant.getStart(), variant.getEnd(), sampleId, isPass);
            for (Map.Entry<Integer, Integer> entry : alleleCount.entrySet()) {
                Integer alleleId = entry.getKey();
                String[] refAlt = buildRefAlt(variant, secondaryAlternates, alleleId);
                Integer start = getAlleleStart(variant, secondaryAlternates, alleleId);
                Integer end = getAlleleEnd(variant, secondaryAlternates, alleleId);
                VariantType alleleType = getAlleleType(variant, secondaryAlternates, alleleId);
                updateAlternateCount(sampleId, entry.getValue(), refAlt, start, end, alleleType);
                updatePassAnnotation(start, end, sampleId, isPass);
                int vMin = Math.min(variant.getStart(), variant.getEnd());
                int min = Math.min(start, end);
                int vMax = Math.max(variant.getStart(), variant.getEnd());
                int max = Math.max(start, end);
                if (vMin != min) {
                    int fillRefCount = refAlleleCnt + (2 - entry.getValue());
                    updateReferenceCount(Math.min(vMin, min), Math.max(vMin, min) - 1, sampleId, fillRefCount);
                }
                if (vMax != max) {
                    int fillRefCount = refAlleleCnt + (2 - entry.getValue());
                    updateReferenceCount(Math.min(vMax, max) + 1, Math.max(vMax, max), sampleId, fillRefCount);
                }
            }
        });
    }

    private int[] calculateRegion(Integer start, Integer end) {
        if (start > end) {
            end = start; // INSERTIONS
        }
        int min = Math.max(start, this.region.getLeft());
        int max = Math.min(end, this.region.getRight() - 1);
        return new int[]{min, max};
    }

    private void updatePassAnnotation(Integer start, Integer end, Integer sampleId, boolean isPass) {
        int[] reg = calculateRegion(start, end);
        for (Integer i = reg[0]; i <= reg[1]; ++i) {
            if (isPass) {
               getPass(i).add(sampleId);
            } else {
                getNotPass(i).add(sampleId);
            }
        }
    }


    public void forEachPosition(BiConsumer<Integer, AlleleCountPosition> consumer) {
        Set<Integer> positions = new HashSet<>();
        positions.addAll(this.referenceToGtToSamples.keySet());
        positions.addAll(this.alternateToGtToSamples.keySet());
        positions.forEach(position -> {
            AlleleCountPosition var = buildPositionCount(position);
            if (Objects.nonNull(var)) {
                consumer.accept(position, var);
            }
        });
    }

    public void forEachVariantPosition(Consumer<Integer> consumer) {
        Set<Integer> positions = new HashSet<>();
        positions.addAll(this.variantToGtToSamples.keySet());
        positions.forEach(consumer);
    }

    public void forEachVariant(Integer position, BiConsumer<String, AlleleCountPosition> consumer) {
        Map<String, Map<Integer, Set<Integer>>> variants = getVariant(position);
        variants.keySet().forEach(variant -> {
            AlleleCountPosition var = buildVariantCount(position, variant);
            if (Objects.nonNull(var)) {
                consumer.accept(variant, var);
            }
        });
    }

    public AlleleCountPosition buildVariantCount(Integer position, String variant) {
        AtomicBoolean hasData = new AtomicBoolean(false);
        AlleleCountPosition count = new AlleleCountPosition();
        getVariant(position, variant).forEach((allele, ids) -> {
            if (!ids.isEmpty()) {
                hasData.set(true);
                count.getAlternate().put(allele, new ArrayList<>(ids));
            }
        });
        if (!hasData.get()) { // NO DATA
            return null;
        }
        return count;
    }

    public AlleleCountPosition buildPositionCount(Integer position) {
        AtomicBoolean hasData = new AtomicBoolean(false);
        AlleleCountPosition count = new AlleleCountPosition();
        getReference(position).forEach((allele, ids) -> {
            if (!ids.isEmpty()) {
                hasData.set(true);
                count.getReference().put(allele, new ArrayList<>(ids));
            }
        });
        getAlt(position).forEach((alt, map) -> map.forEach((allele, ids) -> {
            if (!ids.isEmpty()) {
                hasData.set(true);
                count.getAltMap().computeIfAbsent(alt, k -> new HashMap<>()).put(allele, new ArrayList<>(ids));
            }
        }));
        if (!getNotPass(position).isEmpty()) {
            hasData.set(true);
            count.getNotPass().addAll(getNotPass(position));
        }
        // PASS not needed
        count.getPass().addAll(getPass(position));
        if (!hasData.get()) { // NO DATA
            return null;
        }
        return count;
    }

    public static VariantType getAlleleType(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case 0:
            case 1: return var.getType();
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return alt.getType();
        }
    }

    public static Integer getAlleleStart(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case 0:
            case 1: return var.getStart();
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return alt.getStart();
        }
    }
    public static Integer getAlleleEnd(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case 0:
            case 1: return var.getEnd();
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return alt.getEnd();
        }
    }

    public static String[] buildRefAlt(Variant var, List<AlternateCoordinate> secondaryAlternates, Integer allele) {
        switch (allele) {
            case 0: return new String[0];
            case 1: return new String[] { var.getReference(), var.getAlternate()};
            default:
                AlternateCoordinate alt = secondaryAlternates.get(allele - 2);
                return new String[] { alt.getReference(), alt.getAlternate()};
        }
    }
    public static String buildVariantId(String[] refAlt) {
        return buildVariantId(StringUtils.EMPTY, refAlt);
    }

    public static String buildVariantId(String prefix, String[] refAlt) {
        if (refAlt.length == 0) {
            return StringUtils.EMPTY; // Reference
        }
        if (refAlt.length != 2) {
            throw new IllegalStateException("RefAlt array expected to be of length 2: " + Arrays.toString(refAlt));
        }
        return buildVariantId(prefix, refAlt[0], refAlt[1]);
    }
    public static String buildVariantId(String prefix, String ref, String alt) {
        return prefix + ref + "_" + alt;
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

    protected void updateReferenceCount(Integer start, Integer end, Integer sampleId, Map<Integer, Integer> alleleCount) {
        if (alleleCount.containsKey(NO_CALL)) {
            int[] reg = calculateRegion(start, end);
            for (Integer i = reg[0]; i <= reg[1]; ++i) {
                getReference(i, NO_CALL).add(sampleId);
            }
            alleleCount.remove(NO_CALL);
        } else if (alleleCount.containsKey(REF_IDX)) {
            Integer refCallCnt = alleleCount.remove(REF_IDX);
            updateReferenceCount(start, end, sampleId, refCallCnt);
        }
    }

    public Map<Integer, Set<Integer>> getReference(Integer i) {
        return referenceToGtToSamples
                .computeIfAbsent(i, (k) -> new HashMap<>());
    }

    public Collection<Integer> getReference(Integer i, Integer alleles) {
        return getReference(i).computeIfAbsent(alleles, (k) -> new HashSet<>());
    }

    public Map<String, Map<Integer, Set<Integer>>> getVariant(Integer position) {
        return variantToGtToSamples.computeIfAbsent(position, (k) -> new HashMap<>());
    }

    public Map<Integer, Set<Integer>> getVariant(Integer position, String var) {
        return getVariant(position)
                .computeIfAbsent(var, (k) -> new HashMap<>());
    }

    public Collection<Integer> getVariant(Integer position, String var, Integer alleles) {
        return getVariant(position, var)
                .computeIfAbsent(alleles, (k) -> new HashSet<>());
    }


    public Map<String, Map<Integer, Set<Integer>>> getAlt(Integer position) {
        return alternateToGtToSamples.computeIfAbsent(position, (k) -> new HashMap<>());
    }

    public Map<Integer, Set<Integer>> getAlt(Integer position, String var) {
        return getAlt(position).computeIfAbsent(var, (k) -> new HashMap<>());
    }

    public Collection<Integer> getAlt(Integer position, String var, Integer alleles) {
        return getAlt(position, var).computeIfAbsent(alleles, (k) -> new HashSet<>());
    }

    public Set<Integer> getPass(Integer position) {
        return this.passPosition.computeIfAbsent(position, k -> new HashSet<>());
    }

    public Set<Integer> getNotPass(Integer position) {
        return this.notPassPosition.computeIfAbsent(position, k -> new HashSet<>());
    }

    protected void updateReferenceCount(Integer start, Integer end, Integer sampleId, Integer refAlleleCnt) {
        int[] reg = calculateRegion(start, end);
        boolean insertion = start > end;

        if (insertion) {
            refAlleleCnt *= -1; // negative for INSERTION
            refAlleleCnt += NO_CALL; // offset of -1 (-2,-3,...)
        }
        for (Integer i = reg[0]; i <= reg[1]; ++i) {
            getReference(i, refAlleleCnt).add(sampleId);
        }
    }

    protected void updateAlternateCount(Integer sampleId, Integer alleleCnt, String[] refAlt, Integer start, Integer end,
                                        VariantType alleleType) {
        String vid = buildVariantId(refAlt);
        // Register actual variants
        if (isInRegion(start)) {
            getVariant(start, vid, alleleCnt).add(sampleId);
        }
        String delStar = DEL_SYMBOL;

        if (alleleType.equals(VariantType.INDEL) && start > end) {
            alleleType = VariantType.INSERTION;
        }
        if (alleleType.equals(VariantType.INSERTION)) {
            delStar = INS_SYMBOL;
        }

        // Register * for overlapping regions
        switch (alleleType) {
            case SNV:
            case SNP:
                if (isInRegion(start)) { // Add SNPs also to Reference position
                    String alt = refAlt[1];
                    getAlt(start, alt, alleleCnt).add(sampleId);
                }
                break;
            case INSERTION:
            case DELETION:
            case INDEL:
            case MNV:
            case MNP:
                // Register * for overlapping regions
                int[] reg = calculateRegion(start, end);
                for (int i = reg[0]; i <= reg[1]; ++i) {
                    Collection<Integer> sampleIds = getAlt(i, delStar, alleleCnt);
                    int tmpCnt = alleleCnt;
                    while (sampleIds.remove(sampleId)) {
                        ++tmpCnt;
                        sampleIds = getAlt(i, delStar, tmpCnt);
                    }
                    sampleIds.add(sampleId);
                }
                break;
            default:
                // do nothing for SNPs, ...
                break;
        }
    }

    private boolean isInRegion(Integer start) {
        return start >= this.region.getLeft() && start < this.region.getRight();
    }

    public void onlyLeaveSparseRepresentation(int startPos, int nextStartPos) {
        for (int i = startPos; i < nextStartPos; i++) {
            onlyLeaveSparseRepresentation(i);
        }
        Predicate<Integer> outOfRangeFilter = i -> i < startPos || i >= nextStartPos;

        removeOutOfRange(this.referenceToGtToSamples, outOfRangeFilter);
        removeOutOfRange(this.alternateToGtToSamples, outOfRangeFilter);
        removeOutOfRange(this.variantToGtToSamples, outOfRangeFilter);
        removeOutOfRange(this.passPosition, outOfRangeFilter);
        removeOutOfRange(this.notPassPosition, outOfRangeFilter);
    }

    private void removeOutOfRange(Map<Integer, ?> map, Predicate<Integer> filter) {
        List<Integer> outOfRange = map.keySet().stream().filter(filter).collect(Collectors.toList());
        outOfRange.forEach(i -> map.remove(i));
    }

    private void onlyLeaveSparseRepresentation(int position) {
        // remove HOM_REF variants
        getReference(position).remove(2);
        this.passPosition.remove(position);
        if (this.referenceToGtToSamples.get(position).isEmpty()) {
            this.referenceToGtToSamples.remove(position);
        }
    }

    /**
     * Check each position between start and end, if there are data available for the provided samples.
     * If no GT is available, a nocall is set.
     * If no FILTER is available, NOT-PASS is set.
     * @param expectedSamples  Samples to check.
     * @param startPos     Start position (Inclusive).
     * @param nextStartPos End position (exclusive).
     */
    public void fillNoCalls(Collection<String> expectedSamples, long startPos, long nextStartPos) {
        long timeOld = 0;
        long timeNew = 0;
        Set<Integer> expTemplate = expectedSamples.stream().map(s -> getSampleId(s)).collect(Collectors.toSet());
        for (int i = (int) startPos; i < nextStartPos; ++i) {
            BiConsumer<Collection<Integer>, Set<Integer>> removeFunction = (set, noCalls) -> noCalls.removeAll(set);

            Set<Integer> noCalls = new HashSet<>(expTemplate);
            getReference(i).values().forEach(set -> removeFunction.accept(set, noCalls));
            getAlt(i).values().forEach(b -> b.values().forEach(set -> removeFunction.accept(set, noCalls)));
            // add to nocall if not empty
            if (!noCalls.isEmpty()) {
                getReference(i, NO_CALL).addAll(noCalls);
                getNotPass(i).addAll(noCalls);
            }
        }
        log.info("Runtime old: [{}]; new: [{}]", timeOld, timeNew);
    }

    private Integer getSampleId(String sampleName) {
        return this.sampleNameToSampleId.get(sampleName);
    }

    private boolean isPassFilter(StudyEntry studyEntry) {
        return StringUtils.equals(getFilterValue(studyEntry), PASS_VALUE);
    }

    private String getFilterValue(StudyEntry studyEntry) {
        if (studyEntry.getFiles().isEmpty()) {
            return DEFAULT_ANNOTATION_FILTER_VALUE;
        }
        return studyEntry.getFiles().get(0).getAttributes()
                .getOrDefault(ANNOTATION_FILTER, DEFAULT_ANNOTATION_FILTER_VALUE);
    }

}
