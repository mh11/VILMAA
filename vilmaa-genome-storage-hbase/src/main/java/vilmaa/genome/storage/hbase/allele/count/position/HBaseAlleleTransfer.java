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

import vilmaa.genome.storage.hbase.VariantHbaseUtil;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleCombiner;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.Variant;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Extract variants and transfer allele count to variant table.
 * Extracted logic from mapper.
 * Created by mh719 on 21/03/2017.
 */
public class HBaseAlleleTransfer {

    protected volatile AlleleCombiner alleleCombiner;

    protected volatile Map<Integer, Map<Integer, Integer>> deletionEnds = new HashMap<>();

    public HBaseAlleleTransfer(Set<Integer> sampleIds) {
        this.alleleCombiner = new AlleleCombiner(sampleIds);
    }

    public void resetNewChromosome() {
        this.deletionEnds.clear();
    }

    public void process(AlleleCountPosition refBean, List<Pair<AlleleCountPosition, Variant>> buffer, BiConsumer<Variant, AlleleCountPosition> consumer) {
        Collections.sort(buffer, (a, b) -> {
            int cmp = Integer.compare(
                    Math.abs(a.getRight().getStart() - a.getRight().getEnd()),
                    Math.abs(b.getRight().getStart() - b.getRight().getEnd()));
            if (cmp == 0) {
                cmp = a.getRight().getEnd().compareTo(b.getRight().getEnd());
            }
            if (cmp == 0) {
                cmp = b.getRight().getLength().compareTo(a.getRight().getLength());
            }
            return cmp;
        });

        // Create Overlap Index
        Consumer<Pair<AlleleCountPosition, Variant>> registerOverlapFunction = pair -> {
            AlleleCountPosition toBean = pair.getLeft();
            // process all variants as normal
            Variant variant = pair.getRight();
            // update indel covered regions
            addRegionOverlapIfRequired(variant, toBean);
        };

        // process data
        Consumer<Pair<AlleleCountPosition, Variant>> combineFunction = pair -> {
            AlleleCountPosition toBean = pair.getLeft();
            // process all variants as normal
            Variant variant = pair.getRight();
            transfer(variant, refBean, toBean);
            consumer.accept(variant, toBean);
        };

        Predicate<Pair<AlleleCountPosition, Variant>> isIndelFunction = p -> VariantHbaseUtil.isInsertion(p.getRight());
        Predicate<Pair<AlleleCountPosition, Variant>> isNotIndelFunction = i -> !isIndelFunction.test(i);

        // Only INDELs first -> There is no overlap with Deletions starting at same position
        buffer.stream().filter(isIndelFunction).forEach(registerOverlapFunction);
        buffer.stream().filter(isIndelFunction).forEach(combineFunction);

        // all the others
        buffer.stream().filter(isNotIndelFunction).forEach(registerOverlapFunction);
        buffer.stream().filter(isNotIndelFunction).forEach(combineFunction);
    }

    protected void transfer(Variant variant, AlleleCountPosition from, AlleleCountPosition to) {
        this.alleleCombiner.combine(variant, from, to, this.deletionEnds);
    }

    protected void addRegionOverlapIfRequired(Variant variant, AlleleCountPosition toBean) {
        BiConsumer<Map<Integer, Map<Integer, Integer>>, AlleleCountPosition> overlapFunction = (map, bean) -> {
            Map<Integer, Integer> endMap = map.computeIfAbsent(variant.getEnd(), f -> new HashMap<>());
            bean.getAlternate().forEach((position, ids) -> ids.forEach(sid -> {
                endMap.put(sid, endMap.getOrDefault(sid, 0) + position);
            }));
        };

        switch (variant.getType()) {
            case SNV:
            case SNP:
                break; // do nothing
            case MNP:
            case MNV:
            case INDEL:
            case INSERTION:
            case DELETION:
            case MIXED:
                overlapFunction.accept(this.deletionEnds, toBean);
                break;
            default:
                throw new IllegalStateException("Currently not support: " + variant.getType());

        }
    }

}
