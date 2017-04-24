package diva.genome.analysis.filter;

import diva.genome.storage.models.alleles.avro.AlleleVariant;
import diva.genome.storage.models.alleles.avro.VariantStats;

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
