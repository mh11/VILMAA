package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 03/02/2017.
 */
public class AlleleCombiner {
    private static final Set<String> BASES = new HashSet<>(Arrays.asList("A", "T", "G", "C"));

    private final Set<Integer> sampleIds;

    public AlleleCombiner(Set<Integer> sampleIds) {
        this.sampleIds = sampleIds;
    }

    public void combine(Variant variant, AlleleCountPosition from, AlleleCountPosition to,
                        Map<Integer, Map<Integer, Integer>> overlaps) {
        VariantType type = ensureKnownType(variant);
        ensureVariants(to);
        copyNotPass(from, to);
        Map<Integer, Integer> sidToAllele = fullyCovered(variant, overlaps);
        switch (type) {
            case SNV:
                foreach(from, (k, v) -> copySnv(k, variant.getAlternate(), v, to));
                break;
            case INSERTION:
                foreach(from, (k, v) -> copyInsertion(k, v, from, to, sidToAllele));
                break;
            case DELETION:
            case MNV:
                foreach(from, (k, v) -> copyDeletion(k, v, from, to, sidToAllele));
                break;
            default:
                break;
        }
        copyReference(from, to, type);
    }

    private void copyReference(AlleleCountPosition fromCount, AlleleCountPosition toCount, VariantType type) {
        Map<Integer, List<Integer>> from = fromCount.getReference();
        ArrayList<Integer> alleles = new ArrayList<>(from.keySet());
        Map<Integer, List<Integer>> to = toCount.getReference();
        Map<Integer, Integer> alternateCount = mapSampleidToAlleleCnt(toCount.getAlternate());

        Map<Integer, Integer> countAlts = new HashMap<>();
        toCount.getAltMap().forEach((alt, map) -> map.forEach((allele, ids) -> ids.forEach(id -> {
            countAlts.put(id, countAlts.getOrDefault(id, 0));
        })));

        // get normal reference first
        Collections.sort(alleles);
        Collections.reverse(alleles);

        Map<Integer, Integer> toMap = mapSampleidToAlleleCnt(to);
        for (Integer fromAllelCnt : alleles) {
            Set<Integer> idList = new HashSet<>(from.get(fromAllelCnt)); // Remove possible duplicates
            for (Integer sid : idList) {
                Integer toAlleleCnt = toMap.getOrDefault(sid, 0);
                Integer toAltCnt = countAlts.getOrDefault(sid, 0);

                if (toAlleleCnt == 0 && fromAllelCnt == HBaseAlleleCalculator.NO_CALL) {
                    toMap.put(sid, fromAllelCnt); // always transfer NOCALL
                } else if (type.equals(VariantType.INSERTION)) {
                    if (toAlleleCnt == 0) {
                        if (toAltCnt > 0 && fromAllelCnt > 0) { // alt exist with high allele count
                            int value = Math.abs(toAltCnt - fromAllelCnt);
                            toMap.put(sid, value); // ignore INSERTION reference calls
                        } else if (fromAllelCnt > HBaseAlleleCalculator.NO_CALL) {
                            // ignore INSERTION REF calls and don't overwrite other REF calls.
                            toMap.put(sid, fromAllelCnt);
                        }
                    }
                } else if (fromAllelCnt <= HBaseAlleleCalculator.NO_CALL && toAlleleCnt > 0) {
                    toMap.put(sid, toAlleleCnt); // ignore INSERTION reference calls
                } else {
                    toMap.put(sid, toAlleleCnt + fromAllelCnt);
                }
            }
        }
        Map<Integer, Set<Integer>> refAlleleMap = mapAlleleCntToSampleid(toMap);
        refAlleleMap.remove(2); // remove hom_ref

        to.clear(); // reset
        refAlleleMap.forEach((k, v) -> to.put(k, new ArrayList<>(v)));
    }

    private Map<Integer, Integer> fullyCovered(Variant variant, Map<Integer, Map<Integer, Integer>> overlaps) {
        Integer start = variant.getStart();
        Integer end = variant.getEnd();
                Map<Integer, Integer> retMap = new HashMap<>();
        overlaps.keySet().stream()
                .filter(i -> start > end ? (i >= end && i < start) : (i >= end && i >= start))
                .forEach(pos ->
                        overlaps.get(pos).forEach((k, v) -> retMap.put(k, retMap.getOrDefault(k, 0) + v)));
        return retMap;
    }

    private void removeCurrentVariantCalls(Map<Integer, Integer> from, Map<Integer, List<Integer>> to) {
        to.forEach((allele, sidlst) -> sidlst.forEach((sid) -> {
            Integer cnt = from.getOrDefault(sid, 0);
            if (cnt > 0) {
                cnt -= allele;
                if (cnt > 0) {
                    from.put(sid, cnt);
                } else {
                    from.remove(sid);
                }
            }
        }));
    }

    private void copyDeletion(String fromAlt, Map<Integer, List<Integer>> map,
                              AlleleCountPosition from, AlleleCountPosition to,
                              Map<Integer, Integer> overlaps) {
        if (fromAlt.equals(HBaseAlleleCalculator.DEL_SYMBOL)) {
            // remove current deletion information
            removeCurrentVariantCalls(overlaps, to.getAlternate());
            // add
            transfer(fromAlt, mapAlleleCntToSampleid(overlaps), to);
        } else if (BASES.contains(fromAlt)) { // A T G C
            transfer(map, to.getReference());
        } else if (fromAlt.equals(HBaseAlleleCalculator.INS_SYMBOL)) {
            // ignore -> references before / after are represented as 0/0
            Map<Integer, Integer> alternate = mapSampleidToAlleleCnt(to.getAlternate());
            Map<Integer, Integer> insertions = mapSampleidToAlleleCnt(map);
            Map<Integer, Integer> toReference = mapSampleidToAlleleCnt(to.getReference());

            // only reference calls > 0 (no NO_CALLS or INDEL ref calls)
            Map<Integer, Integer> fromReference = mapSampleidToAlleleCnt(
                    from.getReference().entrySet().stream().filter(e -> e.getKey() > 0)
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

            insertions.forEach((sid, allele) -> { // TODO still needed?
                // Transfer INDEL as Reference call
                if (alternate.containsKey(sid) && !toReference.containsKey(sid) && !fromReference.containsKey(sid)) {
                    to.getReference().computeIfAbsent(allele, k -> new ArrayList<>()).add(sid);
                }
            });
        } else {
            throw new IllegalStateException("Unexpected option. Not implemented yet: " + fromAlt);
        }

    }

    private void copyInsertion(String fromAlt, Map<Integer, List<Integer>> map,
                               AlleleCountPosition from, AlleleCountPosition to,
                               Map<Integer, Integer> overlaps) {
        if (fromAlt.equals(HBaseAlleleCalculator.INS_SYMBOL)) {
            // remove current Insertion information
            removeCurrentVariantCalls(overlaps, to.getAlternate());
            // add
            transfer(fromAlt, mapAlleleCntToSampleid(overlaps), to);
            // move reference count of insertions to reference count.
            from.getReference().forEach((allele, ids) -> {
                if (allele < HBaseAlleleCalculator.NO_CALL) {
                    to.getReference().put(((allele - HBaseAlleleCalculator.NO_CALL) * -1), ids);
                }
            });
        } else  if (fromAlt.equals(HBaseAlleleCalculator.DEL_SYMBOL)) {
            transfer(fromAlt, map, to);
        } else if (BASES.contains(fromAlt)) { // A T G C
            // that are SNVs -> add them to reference.
            transfer(map, to.getReference());
        } else {
            throw new IllegalStateException("Unexpected option. Not implemented yet: " + fromAlt);
        }
    }


    private void copySnv(String fromAlt, String toAlt, Map<Integer, List<Integer>> map, AlleleCountPosition to) {
        if (fromAlt.equals(toAlt)) {
            return; // same variant -> ignore
        } else if (fromAlt.equals(HBaseAlleleCalculator.INS_SYMBOL)) {
            // Not sure if it's best to add TODO Check
            transfer(fromAlt, map, to);
        } else  if (fromAlt.equals(HBaseAlleleCalculator.DEL_SYMBOL)) {
            transfer(fromAlt, map, to);
        } else if (BASES.contains(fromAlt)) { // A T G C (other than current)
            transfer(fromAlt, map, to);
        } else {
            throw new IllegalStateException("Unexpected option. Not implemented yet: " + fromAlt);
        }
    }

    private void foreach(AlleleCountPosition from, BiConsumer<String, Map<Integer, List<Integer>>> consumer) {
        from.getAltMap().forEach((variant, map) -> consumer.accept(variant, map));
    }

    private void transfer(String fromAlt, Map<Integer, ? extends Collection<Integer>> map, AlleleCountPosition to) {
        transfer(map, to.getAltMap().computeIfAbsent(fromAlt, x -> new HashMap<>()));
    }

    private void transfer(Map<Integer, ? extends Collection<Integer>> from, Map<Integer, List<Integer>> to) {
        from.forEach((k, v) -> to.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
    }

    protected Map<Integer, Integer> mapSampleidToAlleleCnt(Map<Integer, List<Integer>> toVariant) {
        Map<Integer, Integer> sidToAllele = new HashMap<>();
        toVariant.forEach((allele, sidlst) -> sidlst.forEach((sid) ->  {
            Integer cnt = sidToAllele.getOrDefault(sid, 0);
            sidToAllele.put(sid, allele + cnt);
        }));
        return sidToAllele;
    }

    protected Map<Integer, Set<Integer>> mapAlleleCntToSampleid(Map<Integer, Integer> toVariant) {
        Map<Integer, Set<Integer>> coveredMap = new HashMap<>();
        toVariant.forEach((k, v) -> coveredMap.computeIfAbsent(v, x -> new HashSet<>()).add(k));
        return coveredMap;
    }

    VariantType ensureKnownType(Variant variant) {
        switch (variant.getType()) {
            case SNV:
            case SNP:
                return VariantType.SNV;
            case MNP:
            case MNV:
                return VariantType.MNV;
            case INSERTION:
            case DELETION:
                return variant.getType();
            case INDEL:
                if (variant.getStart() <= variant.getEnd()) {
                    return VariantType.DELETION;
                }
                return VariantType.INSERTION;
            default:
                throw new IllegalStateException("Type not supported: " + variant.getType());
        }
    }

    private void copyNotPass(AlleleCountPosition from, AlleleCountPosition to) {
        to.getNotPass().addAll(filterForIndexed(from.getNotPass()));
    }

    private void ensureVariants(AlleleCountPosition to) {
        if (to.getAlternate().isEmpty()) {
            throw new IllegalStateException("Target allele collection has no variant!");
        }
    }

    private Set<Integer> filterForIndexed(Collection<Integer> sids) {
        Set<Integer> indexed = new HashSet<>(sids); // remove possible duplicated IDs
        indexed.retainAll(this.sampleIds); // Only count current indexed samples.
        return indexed;
    }

}
