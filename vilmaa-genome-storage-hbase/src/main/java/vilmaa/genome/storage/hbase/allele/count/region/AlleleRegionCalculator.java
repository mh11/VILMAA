package vilmaa.genome.storage.hbase.allele.count.region;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.storage.hbase.allele.count.position.AbstractAlleleCalculator;
import vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator;
import vilmaa.genome.util.PointRegion;
import vilmaa.genome.util.Region;
import vilmaa.genome.util.RegionImpl;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Calculates sample specific allele count, depth and pass for regions from a {@link Variant}.
 * Created by mh719 on 17/03/2017.
 */
public class AlleleRegionCalculator extends AbstractAlleleCalculator {

    private AlleleRegionStore store;

    public AlleleRegionCalculator(String studyId, Map<String, Integer> sampleNameToSampleId, int start, int endInclusive) {
        this(studyId, sampleNameToSampleId, new AlleleRegionStore(start, endInclusive));
    }

    public AlleleRegionCalculator(String studyId, Map<String, Integer> sampleNameToSampleId, AlleleRegionStore store) {
        super(store.getTargetRegion().getStart(), store.getTargetRegion().getEnd(), studyId, sampleNameToSampleId, true);
        this.store = store;
    }

    public AlleleRegionStore getStore() {
        return store;
    }

    private PositionInfo buildPositionInfo(Variant variant) {
        StudyEntry se = variant.getStudy(studyId);
        boolean isPass = isPassFilter(se);
        PositionInfo positionInfo = new PositionInfo();
        List<AlternateCoordinate> secondaryAlternates = se.getSecondaryAlternates();
        Integer gtPos = se.getFormatPositions().get(VariantMerger.GT_KEY);
        Integer dpPos = se.getFormatPositions().get(DP_KEY);
        Integer adPos = se.getFormatPositions().get(AD_KEY);
        List<List<String>> samplesData = se.getSamplesData();
        se.getSamplesPosition().forEach((sampleName, sampPos) -> {
            Integer sampleId = this.getSampleId(sampleName);
            Map<Integer, AlleleInfo> alleleCount = getAlleleCount(samplesData.get(sampPos), gtPos, dpPos, adPos);
            AlleleInfo refAllele = alleleCount.getOrDefault(REF_IDX, new AlleleInfo(0, 0));
            for (Map.Entry<Integer, AlleleInfo> entry : alleleCount.entrySet()) {
                AlleleInfo currInfo = entry.getValue();
                currInfo.addSampleId(sampleId);
                currInfo.setPass(isPass);
                Integer alleleId = entry.getKey();
                currInfo.setId(buildRefAlt(variant, secondaryAlternates, alleleId));
                currInfo.setType(getAlleleType(variant, secondaryAlternates, alleleId));
                Region<AlleleInfo> altReg = new RegionImpl<>(currInfo,
                        getAlleleStart(variant, secondaryAlternates, alleleId),
                        getAlleleEnd(variant, secondaryAlternates, alleleId));
                if (currInfo.getType().equals(VariantType.INDEL)) {
                    if (altReg.getStart() > altReg.getEnd()) {
                        currInfo.setType(VariantType.INSERTION);
                    } else {
                        currInfo.setType(VariantType.DELETION);
                    }
                }
                positionInfo.addInfo(sampleId, altReg);
                if (alleleId < 2) {
                    continue;
                }
                // only for Sec Alts - fill reference calls
                int fillRefCount = refAllele.getCount() + (2 - currInfo.getCount());


                AlleleInfo fillInfo = new AlleleInfo(fillRefCount, currInfo.getDepth());
                fillInfo.setPass(isPass);
                fillInfo.addSampleId(sampleId);
                fillInfo.setId(REFERENCE_ALLELE);
                fillInfo.setType(VariantType.DELETION); // most of the time correct (TODO improve)

                // no direct overlap between ALTs
                if (!altReg.overlap(new RegionImpl(null, variant.getStart(), variant.getEnd()), true)) {

                    if (alleleCount.containsKey(1)) {
                        // create REF region for First Alt
                        RegionImpl<AlleleInfo> firstAlt = new RegionImpl<>(new AlleleInfo(fillInfo), variant.getStart(),
                                variant.getEnd());
                        AlleleInfo firstInfo = alleleCount.get(1);
                        firstAlt.getData().setType(firstInfo.getType());
                        positionInfo.addInfo(sampleId, firstAlt);

                    }
                    // create REF region for SecAlt
                    RegionImpl<AlleleInfo> secAlt = new RegionImpl<>(new AlleleInfo(fillInfo), altReg
                            .getStart(), altReg.getEnd());
                    secAlt.getData().setType(currInfo.getType());
                    positionInfo.addInfo(sampleId, secAlt);
                } else {

                    int vMin = variant.getStart();
                    int vMax = Math.max(variant.getStart(), variant.getEnd());
                    int min = altReg.getStart();
                    int max = altReg.getMaxPosition();

                    if (vMin != min) {
                        positionInfo.addInfo(sampleId, new RegionImpl<>(fillInfo, Math.min(vMin, min),
                                Math.max(vMin, min) - 1));
                    }
                    if (vMax != max) {
                        positionInfo.addInfo(sampleId, new RegionImpl<>(fillInfo, Math.min(vMax, max) + 1, Math.max(vMax, max)));
                    }
                }
            }
        });
        return positionInfo;
    }

    @Override
    public void addVariant(Variant variant) {
        PositionInfo positionInfo = buildPositionInfo(variant);
        positionInfo.getSampleAlleleRegionInfos().forEach((k, v) -> this.store.addAll(v));
    }


    @Override
    public Map<Integer, Map<String, AlleleCountPosition>> buildVariantMap() {
        return buildVariantMap(this.region);
    }

    public Map<Integer, Map<String, AlleleCountPosition>> buildVariantMap(Region target) {
        if (target.getStart() > target.getMinPosition()) {
            throw new IllegalStateException("Start has to be lower than end. Expected " + target.getStart() + " <= " + target.getEnd());
        }
        Map<Integer, Map<String, AlleleCountPosition>> map = new HashMap<>();
        this.store.getVariation(target, r -> {
            if (r.getMaxPosition() < target.getStart()) {
                return;
            }
            if (r.getStart() > target.getEnd()) {
                return;
            }
            map.computeIfAbsent(r.getStart(), k -> new HashMap<>())
                    .computeIfAbsent(r.getData().getIdString(), k -> new AlleleCountPosition())
                    .getAlternate().computeIfAbsent(r.getData().getCount(), k -> new ArrayList<>())
                    .addAll(r.getData().getSampleIds());
        });
        return map;
    }

    @Override
    public Map<Integer, AlleleCountPosition> buildReferenceMap() {
        return buildReferenceMap(this.region);
    }

    public Map<Integer, AlleleCountPosition> buildReferenceMap(Region target) {
        Map<Integer, AlleleCountPosition> map = new HashMap<>();
        this.store.getReference(target, r -> {
            int count = r.getData().getCount();
            if (!r.overlap(target, true)) {
                return;
            }
            int start = Math.max(target.getStart(), r.getStart());
            int end = Math.min(target.getEnd(), r.getMaxPosition());

            if (r.getData().getType().equals(VariantType.INSERTION)) {
                count = (count * -1) + NO_CALL;
            }
            for (int i = start; i <= end; i++) {
                map.computeIfAbsent(i, k -> new AlleleCountPosition())
                        .getReference().computeIfAbsent(count, k -> new ArrayList<>())
                        .addAll(r.getData().getSampleIds());
            }
        });
        this.store.getNocall(target, r -> {
            int start = Math.max(target.getStart(), r.getStart());
            int end = Math.min(target.getEnd(), r.getEnd());
            for (int i = start; i <= end; i++) {
                map.computeIfAbsent(i, k -> new AlleleCountPosition())
                        .getReference().computeIfAbsent(NO_CALL, k -> new ArrayList<>())
                        .addAll(r.getData().getSampleIds());
            }
        });
        this.store.getVariation(target, r -> {
            int start = Math.max(target.getStart(), r.getStart());
            int end = Math.min(target.getEnd(), r.getMaxPosition());
            String id = r.getData().getIdString();
            if (r.getData().getType().equals(VariantType.SNV) || r.getData().getType().equals(VariantType.SNP)) {
                id = r.getData().getId()[1]; // ALT as id
            } else if (r.getData().getType().equals(VariantType.DELETION)) {
                id = HBaseAlleleCalculator.DEL_SYMBOL;
            } else if (r.getData().getType().equals(VariantType.MNV)) {
                id = HBaseAlleleCalculator.DEL_SYMBOL;
            } else if (r.getData().getType().equals(VariantType.INSERTION)) {
                id = HBaseAlleleCalculator.INS_SYMBOL;
            } else {
                throw new IllegalStateException("Unexpected type: " + r.getData().getType());
            }
            for (int i = start; i <= end; i++) {
                map.computeIfAbsent(i, k -> new AlleleCountPosition())
                        .getAltMap().computeIfAbsent(id, k -> new HashMap<>())
                        .computeIfAbsent(r.getData().getCount(), k -> new ArrayList<>())
                        .addAll(r.getData().getSampleIds());
            }
        });
        map.forEach((k, acp) -> {
            acp.getAltMap().forEach((id, amap) -> {
                updateAlleleCount(amap);
            });
        });

        map.forEach((pos, count) -> {
            count.getNotPass().addAll(getNotPass(pos));
            count.getPass().addAll(getPass(pos));
        });

        map.forEach((pos, count) -> {
            count.getReference().remove(2); // remove hom_ref calls (compatibility)
        });
        return map;
    }



    /**
     * Find duplicated entries an add allele count for these
     * @param alleleCountMap
     */
    private void updateAlleleCount(Map<Integer, List<Integer>> alleleCountMap) {
        List<Integer> acs = new ArrayList<>(alleleCountMap.keySet());
        Collections.sort(acs); // start with lowest ACs
        for (int i = 0; i < acs.size(); i++) {
            Integer ac = acs.get(i);
            List<Integer> idList = alleleCountMap.get(ac);
            HashSet<Integer> uids = new HashSet<>(idList.size());
            // find repeated entries and get count (count -1) for it
            Map<Integer, Long> cntMap = idList.stream().filter(id -> !uids.add(id))
                    .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
            if (cntMap.isEmpty()) {
                continue;
            }
            // remove repeated entries
            uids.removeAll(cntMap.keySet());
            // fill current list with new values
            idList.clear();
            idList.addAll(uids);
            Collections.sort(idList);

            // move repeated values to new key
            cntMap.forEach((id, cnt) -> {
                Integer newkey = (int) (ac * (cnt + 1)); // + 1 for first appearance in set.
                alleleCountMap.computeIfAbsent(newkey, k -> {
                    // key does not yet exist
                    acs.add(newkey); //add to current list ot iterate over
                    Collections.sort(acs);
                    return new ArrayList<>();
                }).add(id);
            });
        }
    }

    @Override
    public Set<Integer> getPass(Integer position) {
        // not efficient, but will do for compatibility
        Set<Integer> pass = new HashSet<>();
        Consumer<Region<AlleleInfo>> passFunction = r -> {
            Set<Integer> sampleId = r.getData().getSampleIds();
            if (r.getData().isPass()) {
                pass.addAll(sampleId);
            }
        };
        this.store.getVariation(position, passFunction);
        this.store.getNocall(position, passFunction);
        this.store.getReference(position, passFunction);
        return pass;
    }

    @Override
    public Set<Integer> getNotPass(Integer position) {
        Set<Integer> notPass = new HashSet<>();
        Consumer<Region<AlleleInfo>> notPassFunction = r -> {
            if (r.getStart() > position.intValue()) {
                return; // INDEL issue
            }
            if (!r.getData().isPass()) {
                notPass.addAll(r.getData().getSampleIds());
            }
        };
        this.store.getVariation(position, notPassFunction);
        this.store.getNocall(position, notPassFunction);
        this.store.getReference(position, notPassFunction);
        return notPass;
    }

    @Override
    public void onlyLeaveSparseRepresentation(int startPos, int nextStartPos, boolean removePass, boolean
            removeHomRef) {
        // ignore
    }

    @Override
    public void fillNoCalls(Collection<String> expectedSamples, long startPos, long nextStartPos) {
        Set<Integer> sidsOrig = expectedSamples.stream().map(s -> getSampleId(s)).collect(Collectors.toSet());
        Map<Integer, List<Integer>> sidToMissing = new HashMap<>();
        // build up missing regions
        IntStream.range((int) startPos, (int) nextStartPos).forEach(pos -> {
            HashSet<Integer> sids = new HashSet<>(sidsOrig);
            this.store.getInfos(pos, r -> sids.removeAll(r.getData().getSampleIds()));
            sids.forEach(s -> sidToMissing.computeIfAbsent(s, (k) -> new ArrayList<>()).add(pos));
        });
        // add missing regions.
        sidToMissing.forEach((sid, lst) -> {
            if (lst.isEmpty()) {
                return;
            }
            int idx = 0;
            int start = -1;
            int tmp = start;
            while(idx < lst.size()) {
                if (start < 0) {
                    start = lst.get(idx++);
                    tmp = start;
                    continue;
                }
                if ((tmp + 1) == lst.get(idx)) {
                    tmp = lst.get(idx++);
                    continue;
                } else {
                    // submit
                    RegionImpl<AlleleInfo> missing = new RegionImpl<>(
                            new AlleleInfo(1, 0, sid, NO_CALL_ALLELE, VariantType.NO_VARIATION, false),
                            start, tmp);
                    this.store.add(missing);
                    start = -1;
                    tmp = start;
                }
            }
            if (start > 0) {
                // submit
                RegionImpl<AlleleInfo> missing = new RegionImpl<>(
                        new AlleleInfo(1, 0, sid, NO_CALL_ALLELE, VariantType.NO_VARIATION, false),
                        start, tmp);
                this.store.add(missing);
            }
        });
    }

    @Override
    public AlleleCountPosition buildVariantCount(Integer position, String variant) {
        return buildVariantMap(new PointRegion(null, position)).get(position).get(variant);
    }

    @Override
    public AlleleCountPosition buildPositionCount(Integer position) {
        return buildReferenceMap(new PointRegion(null, position)).get(position);
    }
}
