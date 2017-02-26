package diva.genome.analysis.spark.filter;

import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.avro.Score;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 26/02/2017.
 */
public class CaddFilter implements Function<AllelesAvro, Boolean>, Predicate<AllelesAvro>,
        org.apache.spark.api.java.function.Function<AllelesAvro, Boolean> {
    public static final int CADD_CUTOFF = 15;
    public static final String CADD_SCALED = "cadd_scaled";


    @Override
    public Boolean apply(AllelesAvro allelesAvro) {
        if (null == allelesAvro || allelesAvro.getAnnotation() == null
                || allelesAvro.getAnnotation().getFunctionalScore() == null) {
            return false;
        }
        return hasHighCadd(allelesAvro.getAnnotation().getFunctionalScore());
    }

    @Override
    public boolean test(AllelesAvro allelesAvro) {
        return apply(allelesAvro);
    }

    @Override
    public Boolean call(AllelesAvro allelesAvro) throws Exception {
        return apply(allelesAvro);
    }


    public boolean hasHighCadd(List<Score> functionalScores){
        if (null == functionalScores || functionalScores.isEmpty()) {
            return false;
        }
        for (Score score : functionalScores) {
            if (isHighCadd(score)) {
                return true;
            }
        }
        return false;
    }

    public boolean isHighCadd(Score score) {
        if (null == score) {
            return false;
        }
        Double value = score.getScore();
        if (value == null || value < CADD_CUTOFF) { // null or less than 15 (no matter which score)
            return false;
        }
        return StringUtils.equals(score.getSource(), CADD_SCALED);
    }
}
