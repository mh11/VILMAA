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

package vilmaa.genome.storage.hbase.allele.count.position;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.util.Region;
import vilmaa.genome.util.RegionImpl;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
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
public class HBaseAlleleCalculator extends AbstractAlleleCalculator {
    public static final String DEL_SYMBOL = "*";
    public static final String INS_SYMBOL = "+";

    private Logger log = LoggerFactory.getLogger(this.getClass());

    // <position> <AlleleCnt> <SampleIds>: Allele count is -1 (nocall), 0 (other alleles), 1, 2
    private volatile Map<Integer, Map<Integer, Set<Integer>>> referenceToGtToSamples = new HashMap<>();

    // Only record  individuals with that allele
    // <Position> <VAR_ID> <Allele count> <SampleIds>: Allele count is 1,2,...
    private Map<Integer, Map<String, Map<Integer, Set<Integer>>>> variantToGtToSamples = new HashMap<>();
    private Map<Integer, Map<String, Map<Integer, Set<Integer>>>> alternateToGtToSamples = new HashMap<>();

    private final Map<Integer, Set<Integer>> passPosition = new HashMap<>();
    private final Map<Integer, Set<Integer>> notPassPosition = new HashMap<>();

    public HBaseAlleleCalculator(String studyId, Map<String, Integer> sampleNameToSampleId) {
        this(studyId, sampleNameToSampleId, 0, Integer.MAX_VALUE);
    }
    public HBaseAlleleCalculator(String studyId, Map<String, Integer> sampleNameToSampleId, int start, int end) {
        super(start, end, studyId, sampleNameToSampleId, true);
    }

    public void addVariant(Variant variant) {
        StudyEntry se = variant.getStudy(studyId);
        boolean isPass = isPassFilter(se);
        List<AlternateCoordinate> secondaryAlternates = se.getSecondaryAlternates();
        Integer gtPos = se.getFormatPositions().get(VariantMerger.GT_KEY);
        Integer dpPos = se.getFormatPositions().get(DP_KEY);
        Integer adPos = se.getFormatPositions().get(AD_KEY);
        List<List<String>> samplesData = se.getSamplesData();
        se.getSamplesPosition().forEach((sampleName, sampPos) -> {
            Integer sampleId = this.getSampleId(sampleName);
            Map<Integer, AlleleInfo> alleleCount = getAlleleCount(samplesData.get(sampPos), gtPos, dpPos, adPos);
            AlleleInfo refAllele = alleleCount.getOrDefault(REF_IDX, new AlleleInfo(0, 0));
            Region<Map<Integer, AlleleInfo>> region = new RegionImpl<>(alleleCount, variant.getStart(), variant.getEnd());
            updateReferenceCount(sampleId, region);
            updatePassAnnotation(variant.getStart(), variant.getEnd(), sampleId, isPass);
            for (Map.Entry<Integer, AlleleInfo> entry : alleleCount.entrySet()) {
                AlleleInfo currInfo = entry.getValue();
                Integer alleleId = entry.getKey();
                currInfo.setId(buildRefAlt(variant, secondaryAlternates, alleleId));
                currInfo.setType(getAlleleType(variant, secondaryAlternates, alleleId));
                Integer start = getAlleleStart(variant, secondaryAlternates, alleleId);
                Integer end = getAlleleEnd(variant, secondaryAlternates, alleleId);
                Region<AlleleInfo> altReg = new RegionImpl<>(currInfo, start, end);
                if (currInfo.getType().equals(VariantType.INDEL) && start > end) {
                    currInfo.setType(VariantType.INSERTION);
                }
                updateAlternateCount(sampleId, altReg);
                updatePassAnnotation(start, end, sampleId, isPass);
                int vMin = Math.min(variant.getStart(), variant.getEnd());
                int min = Math.min(start, end);
                int vMax = Math.max(variant.getStart(), variant.getEnd());
                int max = Math.max(start, end);
                int fillRefCount = refAllele.getCount() + (2 - currInfo.getCount());
                AlleleInfo fillInfo = new AlleleInfo(fillRefCount, currInfo.getDepth());
                fillInfo.setType(VariantType.NO_VARIATION);
                if (vMin != min) {
                    updateReferenceCount(Math.min(vMin, min), Math.max(vMin, min) - 1, sampleId, fillInfo);
                }
                if (vMax != max) {
                    updateReferenceCount(Math.min(vMax, max) + 1, Math.max(vMax, max), sampleId, fillInfo);
                }
            }
        });
    }

    private int[] calculateRegion(int start, int end) {
        int min = Math.max(start, this.region.getStart());
        end = Math.max(start, end);  // INSERTIONS (start > end)
        int max = Math.min(end, this.region.getEnd());
        return new int[]{min, max};
    }

    protected void updatePassAnnotation(Integer start, Integer end, Integer sampleId, boolean isPass) {
        int[] reg = calculateRegion(start, end);
        for (Integer i = reg[0]; i <= reg[1]; ++i) {
            if (isPass) {
               getPass(i).add(sampleId);
            } else {
                getNotPass(i).add(sampleId);
            }
        }
    }


    @Override
    public Map<Integer,Map<String,AlleleCountPosition>> buildVariantMap() {
        Map<Integer,Map<String,AlleleCountPosition>> map = new HashMap<>();
        this.forEachVariantPosition(position -> this.forEachVariant(position, (var, count) ->
                map.computeIfAbsent(position, x -> new HashMap<>()).put(var, count)
        ));
        return map;
    }

    @Override
    public Map<Integer, AlleleCountPosition> buildReferenceMap() {
        Map<Integer, AlleleCountPosition> map = new HashMap<>();
        this.forEachPosition((position, count) -> map.put(position, count));
        return map;
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

    @Override
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

    @Override
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

    protected void updateReferenceCount(Integer sampleId, Region<Map<Integer, AlleleInfo>> region) {
        if (region.getData().containsKey(NO_CALL)) {
            int[] reg = calculateRegion(region.getStart(), region.getEnd());
            for (Integer i = reg[0]; i <= reg[1]; ++i) {
                getReference(i, NO_CALL).add(sampleId);
            }
            region.getData().remove(NO_CALL);
        } else if ( region.getData().containsKey(REF_IDX)) {
            AlleleInfo refCallCnt =  region.getData().remove(REF_IDX);
            updateReferenceCount(region.getStart(), region.getEnd(), sampleId, refCallCnt);
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

    @Override
    public Set<Integer> getPass(Integer position) {
        return this.passPosition.computeIfAbsent(position, k -> new HashSet<>());
    }

    @Override
    public Set<Integer> getNotPass(Integer position) {
        return this.notPassPosition.computeIfAbsent(position, k -> new HashSet<>());
    }

    protected void updateReferenceCount(Integer start, Integer end, Integer sampleId, AlleleInfo refAlleleCnt) {
        int[] reg = calculateRegion(start, end);
        boolean insertion = start > end;
        AlleleInfo info = refAlleleCnt;
        if (insertion) {
            // negative for INSERTION
            // offset of -1 (-2,-3,...)
            info = new AlleleInfo((info.getCount() * -1) + NO_CALL, info.getDepth());
        }
        for (Integer i = reg[0]; i <= reg[1]; ++i) {
            getReference(i, info.getCount()).add(sampleId);
        }
    }

    protected void updateAlternateCount(Integer sampleId, Region<AlleleInfo> altReg) {
        AlleleInfo alleleInfo = altReg.getData();
        String vid = alleleInfo.getIdString();
        // Register actual variants
        int start = altReg.getStart();
        int end = altReg.getEnd();
        if (isInRegion(start)) {
            getVariant(start, vid, alleleInfo.getCount()).add(sampleId);
        }
        String delStar = DEL_SYMBOL;
        if (alleleInfo.getType().equals(VariantType.INSERTION)) {
            delStar = INS_SYMBOL;
        }

        // Register * for overlapping regions
        switch (alleleInfo.getType()) {
            case SNV:
            case SNP:
                if (isInRegion(start)) { // Add SNPs also to Reference position
                    String alt = alleleInfo.getId()[1];
                    getAlt(start, alt, alleleInfo.getCount()).add(sampleId);
                }
                break;
            case INSERTION:
            case DELETION:
            case MIXED:
            case INDEL:
            case MNV:
            case MNP:
                // Register * for overlapping regions
                int[] reg = calculateRegion(start, end);
                for (int i = reg[0]; i <= reg[1]; ++i) {
                    Collection<Integer> sampleIds = getAlt(i, delStar, alleleInfo.getCount());
                    int tmpCnt = alleleInfo.getCount();
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
        return this.region.overlap(start);
    }

    @Override
    public void onlyLeaveSparseRepresentation(int startPos, int nextStartPos, boolean removePass, boolean removeHomRef) {
        for (int i = startPos; i < nextStartPos; i++) {
            onlyLeaveSparseRepresentation(i, removePass, removeHomRef);
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

    private void onlyLeaveSparseRepresentation(int position, boolean removePass, boolean removeHomRef) {
        // remove HOM_REF variants
        if (removeHomRef) {
            getReference(position).remove(2);
        }
        if (removePass) {
            this.passPosition.remove(position);
        }
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
    @Override
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

}
