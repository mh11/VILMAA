package diva.genome.storage.hbase.allele.stats;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.*;
import static htsjdk.variant.variantcontext.Allele.NO_CALL_STRING;

/**
 * Created by mh719 on 10/02/2017.
 */
public class AlleleStatsCalculator {
    private final Set<Integer> indexed;
    private int indexedSize;
    private HBaseAlleleCountsToVariantConverter variantConverter;

    public AlleleStatsCalculator(Collection<Integer> indxedSamples) {
        this.indexed = new HashSet<>(indxedSamples);
        this.indexedSize = this.indexed.size();
        this.variantConverter = new HBaseAlleleCountsToVariantConverter(null, null);
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
//            throw new IllegalStateException("Unexpected Reference allele found: " + refAlleles);
        }
    }


    /*
i = 1000
(end - start) = 1810
i = 2000
(end - start) = 2240
i = 3000
(end - start) = 3385
i = 4000
(end - start) = 5114
i = 5000
(end - start) = 5960
i = 6000
(end - start) = 7824
i = 7000
(end - start) = 9191
i = 8000
(end - start) = 9497


i = 1000
(end - start) = 1866
i = 2000
(end - start) = 2823
i = 3000
(end - start) = 4012
i = 4000
(end - start) = 5630
i = 5000
(end - start) = 7220
i = 6000
(end - start) = 12409
i = 7000
(end - start) = 10580
i = 8000
(end - start) = 12009



    * */


    /*
    * variantType
    * genotypesCount "0/0":10 "0/1":120 ...
    * genotypesFreq  "0/0":0.1234 ...
    * mgfGenotype
    *
    * missingAlleles
    * missingGenotypes
    *
    * x refAlleleCount
    * x refAlleleFreq
    * x altAlleleCount
    * x altAlleleFreq
    * x maf
    * x mafAllele
    *
    * */

    public VariantStats calculateStats(AlleleCountPosition position, Set<Integer> samples, Variant variant) {
        allExist(samples);
//        Map<Integer, Integer> mapping = new HashMap<>();
//        List<List<Integer>> arr = new ArrayList<>();
//        AlleleCountPosition currAllele = position;

        AlleleCountPosition currAllele = new AlleleCountPosition(position, samples);
        Set<Integer> remaining = new HashSet<>(samples);
        validRefAlleles(currAllele);
        HashMap<Integer, List<Integer>> refMap = new HashMap<>(currAllele.getReference());
        refMap.remove(NO_CALL);
        if (!(variant.getType().equals(VariantType.INDEL) && variant.getStart() > variant.getEnd())) {
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
        int noCall = count(currAllele.getReference().get(NO_CALL), validNoCallSampleId);

        int refCount = count(refMap, validSampleId);
        int homRef = remaining.size() * 2;

//        int total = homRef + refCount + alternateCount + noCall + insCnt + delCnt + aCnt + tCnt + gCnt + cCnt;
        int total = homRef + refCount + alternateCount + insCnt + delCnt + aCnt + tCnt + gCnt + cCnt; // no-call is not an Allele

        // TODO add HomRefs

        VariantStats stats = new VariantStats();
        stats.setNumSamples(samples.size());
        stats.setVariantType(variant.getType());

        // REF allele
        stats.setRefAllele(variant.getReference());
        stats.setRefAlleleCount(homRef + refCount);
        stats.setRefAlleleFreq(((float) homRef + refCount) / total);

        // ALT allele
        stats.setAltAllele(variant.getAlternate());
        stats.setAltAlleleCount(alternateCount);
        stats.setAltAlleleFreq(((float) alternateCount) / total);

        if (stats.getRefAlleleFreq() > stats.getAltAlleleFreq()) {
            stats.setMaf(stats.getAltAlleleFreq());
            stats.setMafAllele(variant.getAlternate());
        } else {
            stats.setMaf(stats.getRefAlleleFreq());
            stats.setMafAllele(variant.getReference());
        }
        addGenotypeCount(stats, homRef, noCallIds, currAllele, variant);
        return stats;
    }

    public void addGenotypeCount(VariantStats stats, int homRef, Set<Integer> noCalls, AlleleCountPosition currCount, Variant variant) {
        Map<String, Integer> cntGTs = new HashMap<>();
        Map<String, Integer> altPos = new HashMap<>();
        List<String> alternates = new ArrayList<>();
        alternates.add(variant.getAlternate());
        Set<Integer> unexplained = new HashSet<>();
        altPos.put(variant.getReference(), 0);
        altPos.put(variant.getAlternate(), 1);
        currCount.getAltMap().forEach((alt, map) -> {
            if (alt.equals(INS_SYMBOL)) {
                alt = DEL_SYMBOL;
            }
            if (!altPos.containsKey(alt)) {
                altPos.put(alt, altPos.size());
                alternates.add(alt);
            }
            map.forEach((k, ids) -> unexplained.addAll(ids));
        });

        stats.setGenotypesCount(new HashMap<>());
        //HomRef GT
        if (homRef > 0) {
            cntGTs.put(Genotype.HOM_REF, homRef / 2);
//            stats.getGenotypesCount().put(new Genotype(Genotype.HOM_REF, variant.getReference(), alternates), homRef);
        }
        // HomVar GT
        if (currCount.getAlternate().containsKey(2)) {
            cntGTs.put(Genotype.HOM_VAR, currCount.getAlternate().get(2).size());
//            stats.getGenotypesCount().put(new Genotype(Genotype.HOM_VAR, variant.getReference(), alternates), currCount.getAlternate().get(2).size());
        }

        if (currCount.getAlternate().containsKey(1)) {
            AtomicInteger cnt = new AtomicInteger(0);
            Set<Integer> oneRef = new HashSet<>(currCount.getReference().containsKey(1) ? currCount.getReference().get(1) : Collections.emptyList());
            currCount.getAlternate().get(1).forEach(sid -> {
                if (oneRef.contains(sid)) {
                    cnt.incrementAndGet();
                } else {
                    unexplained.add(sid);
                }
            });
            if (cnt.get() > 0) {
                cntGTs.put("0/1", cnt.get());
//                stats.getGenotypesCount().put(new Genotype("0/1", variant.getReference(), alternates), cnt.get());
            }
        }
        // NOCALL
        if (!noCalls.isEmpty()) {
            cntGTs.put(Genotype.NOCALL, noCalls.size());
//            stats.getGenotypesCount().put(new Genotype(Genotype.NOCALL, variant.getReference(), alternates), noCalls.size());
        }
        stats.setMissingAlleles(noCalls.size());
        stats.setMissingGenotypes(noCalls.size());
        if (!unexplained.isEmpty()) {
            // Resolve all SecAlt + more complex (which are NOT nocall, homvar, homref, het)
            Map<Integer, String> gts = this.variantConverter.buildGts(unexplained, currCount, altPos, variant);
            unexplained.forEach(sid -> cntGTs.compute(gts.get(sid), (k, v) -> v == null ? 1 : v + 1));
//            gts.forEach((id, str) -> cntGTs.compute(str, (k, v) -> v == null ? 1 : v + 1));
        }
        int total = cntGTs.entrySet().stream()
                .filter(e -> !e.getKey().equals(NO_CALL_STRING))
                .mapToInt(i -> i.getValue().intValue()).sum();
        cntGTs.putIfAbsent("0/0", 0);
        cntGTs.putIfAbsent("0/1", 0);
        cntGTs.putIfAbsent("1/1", 0);
        AtomicReference<Genotype> mgt = new AtomicReference<>();
        AtomicInteger mgtCnt = new AtomicInteger(-1);
        cntGTs.forEach((gt, count) -> {
            Genotype g = new Genotype(gt, variant.getReference(), alternates);
            if (count > 0) {
                stats.addGenotype(g, count);
                if (mgtCnt.get() < 1 || mgtCnt.get() > count) {
                    mgtCnt.set(count);
                    mgt.set(g);
                }
            }
            if (!gt.equals(NO_CALL_STRING)) {
                stats.getGenotypesFreq().put(g, (float) count / total);
            }
        });
        if (mgtCnt.get() > -1) {
            stats.setMgf((float) mgtCnt.get() / total);
            stats.setMgfGenotype(mgt.toString());
        }

//        Map<String, Integer> altsCount = new HashMap<>();
//        BiConsumer<String, Integer> processAltFunction = (symbol, idx) -> {
//            if (!currCount.getAltMap().containsKey(symbol)) {
//                return;
//            }
//            currCount.getAltMap().get(symbol).forEach((allele, ids) -> {
//                if (allele < 0) {
//                    return;
//                }
//                String template = allele == 1 ? idx.toString() : StringUtils.repeat(idx.toString(), "/", allele);
//                ids.forEach((sid) -> {
//                    String gt = unexplainedAlt.remove(sid) != null ? "0/" + template : template;
//                    altsCount.compute(gt, (key, val) -> null == val ? 1 : val + 1);
//                });
//            });
//        };
//
//        if (alts.size() > 0) { // there are SecAlts
//            for (int i = 0; i < alts.size(); i++) {
//                int idx = i + 2; // SecAlt adjustment.
//                String alt = alts.get(i);
//                processAltFunction.accept(alt, idx);
//                if (alt.equals(DEL_SYMBOL)) {
//                    // also check INSERTION
//                }
//            }
//        }
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

    private Map<Integer, Integer> countNoCall(Map<Integer, List<Integer>> count, Set<Integer> remaining) {
        return null;
    }

    private Map<Integer, Integer> count(Map<Integer, List<Integer>> count, Function<Integer, Boolean> validSampleId,
                                        Function<Integer, Boolean> validAllele) {
        return null;
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

}
