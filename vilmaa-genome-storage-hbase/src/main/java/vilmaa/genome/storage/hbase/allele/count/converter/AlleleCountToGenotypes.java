package vilmaa.genome.storage.hbase.allele.count.converter;

import vilmaa.genome.storage.hbase.VariantHbaseUtil;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator.*;

/**
 * Created by mh719 on 24/04/2017.
 */
public class AlleleCountToGenotypes {
    private final Set<Integer> indexed;

    public AlleleCountToGenotypes(Set<Integer> indexed) {
        this.indexed = indexed;
    }


    protected void allExist(Set<Integer> samples) {
        for (Integer sid : samples) {
            if (!indexed.contains(sid)) {
                throw new IllegalStateException("Sample not Indexed!!! " + sid);
            }
        }
    }

    protected void validRefAlleles(AlleleCountPosition position) {
        Set<Integer> refAlleles = new HashSet<>(position.getReference().keySet());
        refAlleles.remove(NO_CALL);
        refAlleles.remove(1);
        if (!refAlleles.isEmpty()) {
            refAlleles.forEach(r -> position.getReference().remove(r));
        }
    }

    private int count(List<Integer> count, Function<Integer, Boolean> validSampleId) {
        int cnt = 0;
        if (null == count) {
            return cnt;
        }
        for (Integer sid : count) {
            if (validSampleId.apply(sid)) {
                ++cnt;
            }
        }
        return cnt;
    }

    private int count(Map<Integer, List<Integer>> count, Function<Integer, Boolean> validSampleId) {
        int cnt = 0;
        if (null == count) {
            return cnt;
        }
        for (Map.Entry<Integer, List<Integer>> e : count.entrySet()) {
            if (e.getKey() < 1) {
                throw new IllegalStateException(
                        "All alleles expected to be > 0!!! Fount " + e.getKey() + " with " + e.getValue());
            }
            cnt += e.getKey() * count(e.getValue(), validSampleId);
        }
        return cnt;
    }
    public GenotypeCollection convert(AlleleCountPosition position, Set<Integer> samples, Variant variant, Consumer<AlleleCountPosition> checkFilteredObject) {
        allExist(samples);
        AlleleCountPosition currAllele = new AlleleCountPosition(position, samples);
        Set<Integer> remaining = new HashSet<>(samples);
        validRefAlleles(currAllele);
        if (null != checkFilteredObject) {
            checkFilteredObject.accept(currAllele);
        }

        HashMap<Integer, List<Integer>> refMap = new HashMap<>(currAllele.getReference());
        refMap.remove(NO_CALL);
        if (!VariantHbaseUtil.isInsertion(variant)) {
            currAllele.getAltMap().remove(INS_SYMBOL); // Ignore insertion for ALL except INSERTION.
        }

        Set<Integer> noCallIds = new HashSet<>();

        Function<Integer, Boolean> validSampleId = (sid) -> {
            remaining.remove(sid);
            return true;
        };

        Function<Integer, Boolean> validNoCallSampleId = (sid) -> {
            boolean isNoCall = remaining.remove(sid);
            if (isNoCall) {
                noCallIds.add(sid);
            }
            return isNoCall;
        };
        int alternateCount = count(currAllele.getAlternate(), validSampleId);
        int insCnt = count(currAllele.getAltMap().get(INS_SYMBOL), validSampleId);
        int delCnt = count(currAllele.getAltMap().get(DEL_SYMBOL), validSampleId);
        int aCnt = count(currAllele.getAltMap().get("A"), validSampleId);
        int tCnt = count(currAllele.getAltMap().get("T"), validSampleId);
        int gCnt = count(currAllele.getAltMap().get("G"), validSampleId);
        int cCnt = count(currAllele.getAltMap().get("C"), validSampleId);
        count(currAllele.getReference().get(NO_CALL), validNoCallSampleId); // needed to remove IDS!!!

        int refCount = count(refMap, validSampleId);

//        Map<String, Integer> cntGTs = new HashMap<>();

        Set<Integer> explained = new HashSet<>();
        Set<Integer> unexplained = new HashSet<>();

        // Prepare ALT index
        GenotypeCollection genotypes = new GenotypeCollection();
        Map<String, Integer> altPos = new HashMap<>();
        genotypes.getAlleles().add(variant.getReference());
        altPos.put(variant.getReference(), 0);
        genotypes.getAlleles().add(variant.getAlternate());
        altPos.put(variant.getAlternate(), 1);
        currAllele.getAltMap().forEach((alt, map) -> {
            if (alt.equals(INS_SYMBOL)) {
                alt = DEL_SYMBOL;
            }
            if (!altPos.containsKey(alt)) {
                altPos.put(alt, altPos.size());
                genotypes.getAlleles().add(alt);
            }
            map.forEach((k, ids) -> unexplained.addAll(ids));
        });

        //HomRef GT
        if (!remaining.isEmpty()) {
            genotypes.getGenotypeToSamples().put(Genotype.HOM_REF, new HashSet<>(remaining));
            explained.addAll(remaining);
        }

        // HomVar GT
        if (currAllele.getAlternate().containsKey(2)) {
            genotypes.getGenotypeToSamples().put(Genotype.HOM_VAR, new HashSet<>(currAllele.getAlternate().get(2)));
            explained.addAll(currAllele.getAlternate().get(2));
        }

        Set<Integer> oneRef = new HashSet<>(currAllele.getReference().containsKey(1) ? currAllele.getReference().get(1) : Collections.emptyList());

        if (currAllele.getAlternate().containsKey(1)) {
            Set<Integer> hetIds = new HashSet<>();
            currAllele.getAlternate().get(1).forEach(sid -> {
                if (oneRef.remove(sid)) {
                    hetIds.add(sid);
                } else {
                    unexplained.add(sid);
                }
            });
            if (!hetIds.isEmpty()) {
                genotypes.getGenotypeToSamples().put(Genotype.HET_REF, hetIds);
                explained.addAll(hetIds);
            }
        }
        unexplained.addAll(oneRef); // add individuals with one reference allele
        // NOCALL
        if (!noCallIds.isEmpty()) {
            genotypes.getGenotypeToSamples().put(Genotype.NOCALL, noCallIds);
            explained.addAll(noCallIds);
        }
        unexplained.removeAll(explained); // make sure all explained are removed
        if (!unexplained.isEmpty()) {
            // Resolve all SecAlt + more complex (which are NOT nocall, homvar, homref, het)
            Map<Integer, String> gts = buildComplexGts(unexplained, currAllele, altPos, variant);
            // add samples to collection for each GT
            gts.forEach((k, v) -> genotypes.getGenotypeToSamples().computeIfAbsent(v, x -> new HashSet<>()).add(k));
        }
        return genotypes;
    }

    public Map<Integer, String> buildComplexGts(Set<Integer> sampleIds, AlleleCountPosition bean, Map<String, Integer> altIndexs, Variant variant) {
        if (sampleIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Integer, String> gts = new HashMap<>();

        BiFunction<Integer, String, String> createRefGenotype = (cnt, gt) -> {
            if (StringUtils.equals(gt, Allele.NO_CALL_STRING) || StringUtils.isBlank(gt)) {
                String retGt = StringUtils.repeat("0", "/", cnt);
                if (StringUtils.isBlank(gt)) {
                    return retGt;
                } else {
                    return gt + "/" + retGt;
                }
            }
            return gt;
        };

        bean.getReference().forEach((alleles, v) -> {
            if (alleles.equals(NO_CALL)) {
                v.forEach(sid -> gts.put(sid, Allele.NO_CALL_STRING));
                return;
            }
            if (!alleles.equals(2)) { //only contains COUNTS
                String gt = createRefGenotype.apply(alleles, StringUtils.EMPTY);
                v.forEach(sid -> gts.put(sid, gt));
                return;
            }
        });

        BiConsumer<String, Map<Integer, List<Integer>>> mapGenotype = (k, a) -> {
            if (a.isEmpty()) {
                return;
            }
            if (StringUtils.equals(k, INS_SYMBOL)) {
                // Only for INSERTIONS
                if (VariantHbaseUtil.isInsertion(variant)) {
                    k = DEL_SYMBOL;
                } else {
                    // Else Ignore
                    return;
                }
            }
            Integer altIdx = altIndexs.get(k);
            if (null == altIdx) {
                throw new IllegalStateException("Problems with " + k + " for " + variant);
            }
            a.forEach((alleles, ids) -> {
                String gtTemplate = altIdx + "";
                for (int i = 1; i < alleles; i++) {
                    gtTemplate += "/" + altIdx;
                }
                String gtT = gtTemplate;
                ids.forEach(sampleId -> {
                    // for each sample ID
                    String gt = gts.get(sampleId);
                    if (StringUtils.isBlank(gt) || StringUtils.equals(gt, Allele.NO_CALL_STRING)) {
                        gt = gtT;
                    } else {
                        gt += "/" + gtT;
                    }
                    gts.put(sampleId, gt);
                });
            });
        };
        bean.getAltMap().forEach(mapGenotype);
        mapGenotype.accept(variant.getAlternate(), bean.getAlternate());
        String homRef = createRefGenotype.apply(2, null);
        sampleIds.forEach(sid -> {
            String gt = gts.get(sid);
            if (StringUtils.isBlank(gt)) {
                gts.put(sid, homRef);
            } else {
                // Order GT
                String[] split = gt.split("/");
                Arrays.sort(split);
                gt = StringUtils.join(split, '/');
                gts.put(sid, gt);
            }
        });
        for (Integer sid : new ArrayList<>(gts.keySet())) {
            if (!sampleIds.contains(sid)) {
                gts.remove(sid);
            }
        }
        return gts;
    }


    public static class GenotypeCollection {
        private final List<String> alleles;
        private final Map<String, Set<Integer>> genotypeToSamples;

        private GenotypeCollection() {
            this.alleles = new ArrayList<>();
            this.genotypeToSamples = new HashMap<>();
        }

        public List<String> getAlleles() {
            return alleles;
        }

        public Map<String, Set<Integer>> getGenotypeToSamples() {
            return genotypeToSamples;
        }
    }

}

