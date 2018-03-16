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
import vilmaa.genome.storage.models.alleles.avro.VariantAnnotation;
import org.apache.commons.codec.binary.StringUtils;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;

import java.util.List;
import java.util.Optional;

/**
 * Filter on Study and Cohort frequency e.g. EXAC study for ALL cohort.
 *
 * Created by mh719 on 02/03/2017.
 */
public class PopulationAlleleFrequencyFilter extends AbstractFilter<AlleleVariant> {
    private final Float af;
    private final String study;
    private final String cohort;

    public PopulationAlleleFrequencyFilter(Float afExclusive, String study, String cohort) {
        this.af = afExclusive;
        this.study = study;
        this.cohort = cohort;
    }


    @Override
    public Boolean doTest(AlleleVariant AlleleVariant) {
        Float exac = extractExacAlleleFrequency(AlleleVariant.getAnnotation());
        return exac < this.af;
    }

    private Float extractExacAlleleFrequency(VariantAnnotation annotation) {
        if (null == annotation) {
            return 0F;
        }
        return extractExacAlleleFrequency(annotation.getPopulationFrequencies());
    }

    private Float extractExacAlleleFrequency(List<PopulationFrequency> populationFrequencies) {
        if (null == populationFrequencies) {
            return 0F;
        }
        Optional<PopulationFrequency> first = populationFrequencies.stream().filter(this::isExacFrequency).findFirst();
        if (!first.isPresent()) {
            return 0F;
        }
        return first.get().getAltAlleleFreq();
    }

    private boolean isExacFrequency(PopulationFrequency populationFrequency) {
        return StringUtils.equals(populationFrequency.getStudy(), this.study)
                && StringUtils.equals(populationFrequency.getPopulation(), this.cohort);
    }

}
