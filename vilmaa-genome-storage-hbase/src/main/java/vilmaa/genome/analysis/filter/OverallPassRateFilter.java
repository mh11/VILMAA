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

package vilmaa.genome.analysis.filter;

import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import vilmaa.genome.storage.models.alleles.avro.VariantStats;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 28/02/2017.
 */
public class OverallPassRateFilter implements Function<AlleleVariant, Boolean>, Predicate<AlleleVariant> {

    private final Pair<Float, Set<String>> cutoffInclusive;
    private final Map<String, Pair<Float, Set<String>>> chromosomeSpecific = new HashMap<>();

    public OverallPassRateFilter(Float cutoffInclusive, Set<String> oprCohort) {
        this.cutoffInclusive = new ImmutablePair<>(cutoffInclusive, oprCohort);
    }

    public void addChromosomeFilter(String chromosome, Float cutoff, Set<String> cohorts) {
        this.chromosomeSpecific.put(chromosome, new ImmutablePair<>(cutoff, cohorts));
    }

    @Override
    public Boolean apply(AlleleVariant AlleleVariant) {
        return test(AlleleVariant);
    }

    @Override
    public boolean test(AlleleVariant AlleleVariant) {
        Pair<Float, Set<String>> filter =
                chromosomeSpecific.getOrDefault(AlleleVariant.getChromosome(), this.cutoffInclusive);
        for (String cohort : filter.getRight()) {
            VariantStats stats = AlleleVariant.getStats().get(cohort);
            if (null != stats && null != stats.getOverallPassrate() && stats.getOverallPassrate() < filter.getLeft()) {
                return false;
            }
        }
        return true;
    }
}
