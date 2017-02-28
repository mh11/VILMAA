package diva.genome.analysis.filter;

import diva.genome.storage.models.alleles.avro.AllelesAvro;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 28/02/2017.
 */
public class OverallPassRateFilter implements Function<AllelesAvro, Boolean>, Predicate<AllelesAvro> {

    private final Float cutoffInclusive;

    public OverallPassRateFilter(Float cutoffInclusive) {
        this.cutoffInclusive = cutoffInclusive;
    }

    @Override
    public Boolean apply(AllelesAvro allelesAvro) {
        return test(allelesAvro);
    }

    @Override
    public boolean test(AllelesAvro allelesAvro) {
        return allelesAvro.getOverallPassRate() >= cutoffInclusive;
    }
}
