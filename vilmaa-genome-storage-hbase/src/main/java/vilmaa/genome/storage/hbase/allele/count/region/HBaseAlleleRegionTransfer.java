package vilmaa.genome.storage.hbase.allele.count.region;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleCombiner;
import vilmaa.genome.util.PointRegion;
import vilmaa.genome.util.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Extract variants from allele count table and transfer information to variant table.
 * Created by mh719 on 20/03/2017.
 */
public class HBaseAlleleRegionTransfer {

    private final Set<Integer> sampleIds;
    private AlleleCombiner oldCombiner;

    public HBaseAlleleRegionTransfer(Set<Integer> sampleIds) {
        this.sampleIds = sampleIds;
        this.oldCombiner = new AlleleCombiner(this.sampleIds);
    }

    public void transfer(String chr, Region target, AlleleRegionStore store, BiConsumer<Variant, AlleleCountPosition> consumer) {
        AlleleRegionCalculator regionCalculator = new AlleleRegionCalculator("", Collections.emptyMap(), store);
        Map<Integer, Map<String, AlleleCountPosition>> allVarMap = regionCalculator.buildVariantMap(target);

        Map<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        allVarMap.forEach((pos, e1) -> e1.forEach((id, cnt) -> {
            buildOverlaps(pos, id, cnt, overlaps);
        }));

        allVarMap.forEach((pos, e1) -> {
            AlleleCountPosition from = regionCalculator.buildReferenceMap(new PointRegion(null, pos)).get(pos);
            e1.forEach((vid, to) -> {
                Variant variant = buildVariant(chr, pos, vid);
                this.oldCombiner.combine(variant, from, to, overlaps);
                consumer.accept(variant, to);
            });
        });
    }

    protected void buildOverlaps(int start, String idstr, AlleleCountPosition toBean, Map<Integer, Map<Integer, Integer>> overlaps) {
        String[] idArr = AlleleInfo.parseVariantId(idstr);
        if (idArr[0].length() == 1 && idArr[1].length() == 1) {
            return; // SNV
        }
        int end = start + idArr[0].length() - 1;

        BiConsumer<Map<Integer, Map<Integer, Integer>>, AlleleCountPosition> overlapFunction = (map, bean) -> {
            Map<Integer, Integer> endMap = map.computeIfAbsent(end, f -> new HashMap<>());
            bean.getAlternate().forEach((position, ids) -> ids.forEach(sid -> {
                endMap.put(sid, endMap.getOrDefault(sid, 0) + position);
            }));
        };
        overlapFunction.accept(overlaps, toBean);
    }


    public static Variant buildVariant(String chr, Integer start, String vid) {
        String[] idArr = AlleleInfo.parseVariantId(vid);
        Integer end =  start + idArr[0].length() - 1;
        VariantType type = VariantType.SNV;
        if (idArr[0].length() == idArr[1].length()) {
            if (idArr[0].length() > 1) {
                type = VariantType.MNV;
            }
        } else if (start > end) {
            type = VariantType.INSERTION;
        } else {
            type = VariantType.DELETION;
        }
        Variant variant = new Variant(chr, start, end, idArr[0], idArr[1]);
        variant.setType(type);
        return variant;
    }

}
