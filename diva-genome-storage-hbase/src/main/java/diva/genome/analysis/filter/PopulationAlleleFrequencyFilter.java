package diva.genome.analysis.filter;

import diva.genome.storage.models.alleles.avro.AllelesAvro;
import diva.genome.storage.models.alleles.avro.VariantAnnotation;
import org.apache.commons.codec.binary.StringUtils;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;

import java.util.List;
import java.util.Optional;

/**
 * Filter on Study and Cohort frequency e.g. EXAC study for ALL cohort.
 *
 * Created by mh719 on 02/03/2017.
 */
public class PopulationAlleleFrequencyFilter extends AbstractFilter<AllelesAvro> {
    private final Float af;
    private final String study;
    private final String cohort;

    public PopulationAlleleFrequencyFilter(Float afExclusive, String study, String cohort) {
        this.af = afExclusive;
        this.study = study;
        this.cohort = cohort;
    }


    @Override
    public Boolean doTest(AllelesAvro allelesAvro) {
        Float exac = extractExacAlleleFrequency(allelesAvro.getAnnotation());
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
