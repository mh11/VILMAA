package vilmaa.genome.analysis.filter;

import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.avro.Score;

import java.util.List;

/**
 * Created by mh719 on 26/02/2017.
 */
public class CaddFilter extends AbstractFilter<AlleleVariant> {
    public static final int CADD_CUTOFF = 15;
    public static final String CADD_SCALED = "cadd_scaled";


    @Override
    public Boolean doTest(AlleleVariant AlleleVariant) {
        if (null == AlleleVariant || AlleleVariant.getAnnotation() == null
                || AlleleVariant.getAnnotation().getFunctionalScore() == null) {
            return false;
        }
        return hasHighCadd(AlleleVariant.getAnnotation().getFunctionalScore());
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
