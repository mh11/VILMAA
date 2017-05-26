package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.VariantHbaseUtil;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator;
import org.apache.commons.lang3.ObjectUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.*;

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
                foreach(from, (k, v) -> copySnv(k, variant.getAlternate(), v, to), variant.getAlternate());
                copyReference(from, to);
                break;
            case INSERTION:
                foreach(from, (k, v) -> copyInsertion(k, v, from, to, sidToAllele), INS_SYMBOL);
                copyInsertionReference(from, to);
                break;
            case DELETION:
            case MIXED:
                foreach(from, (k, v) -> copyDeletion(k, v, from, to, sidToAllele), DEL_SYMBOL);
                copyReference(from, to);
                break;
            case MNV:
                foreach(from, (k, v) -> copyDeletion(k, v, from, to, sidToAllele));
                copyReference(from, to);
                break;
            default:
                break;
        }
    }

    private void copyReference(AlleleCountPosition fromCount, AlleleCountPosition toCount) {
        Map<Integer, List<Integer>> from = fromCount.getReference();
        ArrayList<Integer> alleles = new ArrayList<>(from.keySet());
        Map<Integer, List<Integer>> to = toCount.getReference();

        // get normal reference first
        Collections.sort(alleles);
        Collections.reverse(alleles);

        Map<Integer, Integer> toMap = mapSampleidToAlleleCnt(to);
        for (Integer fromAllelCnt : alleles) {
            Set<Integer> idList = new HashSet<>(from.get(fromAllelCnt)); // Remove possible duplicates
            for (Integer sid : idList) {
                Integer toAlleleCnt = toMap.getOrDefault(sid, 0);

                if (toAlleleCnt == 0 && fromAllelCnt == NO_CALL) {
                    toMap.put(sid, fromAllelCnt); // always transfer NOCALL if nothing else available.
                } else if (fromAllelCnt > NO_CALL) {
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
        if (fromAlt.equals(DEL_SYMBOL)) {
            Map<Integer, Integer> deletions = mapSampleidToAlleleCnt(map);
            overlaps.keySet().forEach(k -> deletions.remove(k)); // remove all known overlaps

            // remove current deletion information
            removeCurrentVariantCalls(overlaps, to.getAlternate());
            // add larger deletions
            transfer(fromAlt, mapAlleleCntToSampleid(overlaps), to);
            // add smaller deletions as reference
            deletions.forEach((sid, allele) -> to.getReference().computeIfAbsent(allele, k -> new ArrayList<>()).add(sid));
        } else if (BASES.contains(fromAlt)) { // A T G C
            transfer(map, to.getReference());
        } else if (fromAlt.equals(INS_SYMBOL)) {
            // ignore -> references before / after are represented as 0/0
            Map<Integer, Integer> alternate = mapSampleidToAlleleCnt(to.getAlternate());
            Map<Integer, Integer> insertions = mapSampleidToAlleleCnt(map);
            Map<Integer, Integer> toReference = mapSampleidToAlleleCnt(to.getReference());
            Map<Integer, Integer> currOtherDels = mapSampleidToAlleleCnt(
                    ObjectUtils.firstNonNull(from.getAltMap().get(DEL_SYMBOL), Collections.emptyMap()));

            // only reference calls > 0 (no NO_CALLS or INDEL ref calls)
            Map<Integer, Integer> fromReference = mapSampleidToAlleleCnt(
                    from.getReference().entrySet().stream().filter(e -> e.getKey() > 0)
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
            AtomicBoolean hasChanged = new AtomicBoolean(false);
            insertions.forEach((sid, allele) -> { // TODO still needed?
                if (fromReference.containsKey(sid)) {
                    return;
                }
                // Transfer INDEL as Reference call
                if (alternate.containsKey(sid) && !toReference.containsKey(sid)) {
                    toReference.put(sid, allele);
                    hasChanged.set(true);
                } else if (!alternate.containsKey(sid) && currOtherDels.containsKey(sid)) {
                    // FIX case, where Insertion + Deletion was called in one variant (SecAlt).
                    toReference.put(sid, toReference.getOrDefault(sid, 0) + allele);
                    hasChanged.set(true);
                }
            });
            if (hasChanged.get()) {
                to.getReference().clear();
                to.getReference().putAll(mapAlleleCntToSampleidList(toReference));
            }
        } else {
            throw new IllegalStateException("Unexpected option. Not implemented yet: " + fromAlt);
        }

    }

    private void copyInsertion(String fromAlt, Map<Integer, List<Integer>> map,
                               AlleleCountPosition from, AlleleCountPosition to,
                               Map<Integer, Integer> overlaps) {
        if (fromAlt.equals(INS_SYMBOL)) {
            // remove current Insertion information
            removeCurrentVariantCalls(overlaps, to.getAlternate());
            // add
            transfer(fromAlt, mapAlleleCntToSampleid(overlaps), to);
            // move reference count of insertions to reference count.
            from.getReference().forEach((allele, ids) -> {
                if (allele < NO_CALL) {
                    to.getReference()
                            .computeIfAbsent(((allele - NO_CALL) * -1), k -> new ArrayList<>())
                            .addAll(ids);
                }
            });
        } else  if (fromAlt.equals(DEL_SYMBOL)) {
            transfer(fromAlt, map, to);
        } else if (BASES.contains(fromAlt)) { // A T G C
            Map<Integer, Integer> currAlts = mapSampleidToAlleleCnt(to.getAlternate());
            Map<Integer, Integer> currRefs = mapSampleidToAlleleCnt(to.getReference());
            Map<Integer, Integer> currOtherInsertions = mapSampleidToAlleleCnt(
                    ObjectUtils.firstNonNull(from.getAltMap().get(INS_SYMBOL), Collections.emptyMap()));

            // that are SNVs -> add them to reference.
            map.forEach((allele, ids) -> {
                List<Integer> refIds = to.getReference().computeIfAbsent(allele, k -> new ArrayList<>());
                ids.forEach((sid) -> {
                    if (!currAlts.containsKey(sid) && !currOtherInsertions.containsKey(sid)) {
                        refIds.add(sid); // only add if not in current (or other INSERTION)  (-> indel overlap!!!)
                    } else if (!currRefs.containsKey(sid)) {
                        // FIX for SNP called as SecAlt -> no reference recorded for INDEL!!!
                        refIds.add(sid);
                    }
                });
            });
        } else {
            throw new IllegalStateException("Unexpected option. Not implemented yet: " + fromAlt);
        }
    }

    private void copyInsertionReference(AlleleCountPosition from, AlleleCountPosition to) {
        // All Insertion reference allele calls already copied over.
        Map<Integer, Integer> toReferenceCount = mapSampleidToAlleleCnt(to.getReference());
        // With sids of current Insertion
        Map<Integer, Integer> toAlternateCount = mapSampleidToAlleleCnt(to.getAlternate());
        Set<Integer> toOtherInsertionIds = new HashSet<>();
        if (to.getAltMap().containsKey(INS_SYMBOL)) {
            to.getAltMap().get(INS_SYMBOL).forEach((k, ids) -> toOtherInsertionIds.addAll(ids));
        }

        // fill other reference calls, which are not in this list.
        from.getReference().forEach((allele, ids) -> {
            if (allele < NO_CALL) {
                ids.forEach((sid) -> {
                    if (!toReferenceCount.containsKey(sid) || toReferenceCount.get(sid) < 0) {
                        throw new IllegalStateException("Insertion reference call should have been transferred!!!");
                    }
                });
                return; // Ignore  - already transferred.
            }
            if (allele.equals(NO_CALL)) {
                // only fill if not set yet
                ids.forEach((sid) -> toReferenceCount.putIfAbsent(sid, NO_CALL));
                return;
            }
            ids.forEach((sid) -> {
                if (!toReferenceCount.containsKey(sid)) {
                    toReferenceCount.put(sid, allele);
                } else if (!toAlternateCount.containsKey(sid) && !toOtherInsertionIds.contains(sid))  {
                    Integer value = toReferenceCount.getOrDefault(sid, 0);
                    if (value.equals(NO_CALL)) {
                        value = allele;
                    } else if (value < 0 && allele > 0) {
                        value = allele; // overwrite (not sure if it makes sense)
                    } else if (value > 0 && allele < 0) {
                        value = value; // overwrite (not sure if it makes sense)
                    } else {
                        value += allele;
                    }
                    toReferenceCount.put(sid, value);
                }
            });
        });
        to.getReference().clear();
        to.getReference().putAll(mapAlleleCntToSampleidList(toReferenceCount));
        to.getReference().remove(2); // remove HomRef
    }


    private void copySnv(String fromAlt, String toAlt, Map<Integer, List<Integer>> map, AlleleCountPosition to) {
        if (fromAlt.equals(toAlt)) {
            return; // same variant -> ignore
        } else if (fromAlt.equals(INS_SYMBOL)) {
            // Not sure if it's best to add TODO Check
            transfer(fromAlt, map, to);
        } else  if (fromAlt.equals(DEL_SYMBOL)) {
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

    private void foreach(AlleleCountPosition from, BiConsumer<String, Map<Integer, List<Integer>>> consumer, String ... preferences) {
        Set<String> preferenceSet = new HashSet<>(Arrays.asList(preferences));
        for (String preference : preferences) {
            if (from.getAltMap().containsKey(preference)) {
                consumer.accept(preference, from.getAltMap().get(preference));
            }
        }
        from.getAltMap().forEach((varId, map) -> {
            if (!preferenceSet.contains(varId)) {
                consumer.accept(varId, map);
            }
        });
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
    protected Map<Integer, List<Integer>> mapAlleleCntToSampleidList(Map<Integer, Integer> toVariant) {
        Map<Integer, List<Integer>> coveredMap = new HashMap<>();
        toVariant.forEach((k, v) -> coveredMap.computeIfAbsent(v, x -> new ArrayList<>()).add(k));
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
            case MIXED:
                return variant.getType();
            case INDEL:
                return VariantHbaseUtil.inferType(variant);
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
