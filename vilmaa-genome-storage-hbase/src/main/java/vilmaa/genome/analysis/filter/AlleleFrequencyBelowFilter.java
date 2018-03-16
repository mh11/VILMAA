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

import java.util.Map;

/**
 * Created by mh719 on 25/02/2017.
 */
public class AlleleFrequencyBelowFilter extends AbstractFilter<AlleleVariant> {
    private final String controlCohort;
    private final Float freqAuto;
    private final Float freqX;

    public AlleleFrequencyBelowFilter(String controlCohort, Float freqAuto, Float freqX) {
        this.controlCohort = controlCohort;
        this.freqAuto = freqAuto;
        this.freqX = freqX;
    }

    private boolean isRareControl(Map<String, VariantStats> stats, Float cutoff) {
        return stats.get(controlCohort).getMaf() < cutoff;
    }
    @Override
    public Boolean doTest(AlleleVariant AlleleVariant) {
        Map<String, VariantStats> stats = AlleleVariant.getStats();
        if (null == stats || stats.isEmpty()) {
            return false;
        }
        return isRareControl(stats, getCutoff(AlleleVariant.getChromosome()));
    }

    private Float getCutoff(String chromosome) {
        switch (chromosome) {
            case "X":
            case "chrX":
                return this.freqX;
            default:
                return this.freqAuto;
        }
    }

}
