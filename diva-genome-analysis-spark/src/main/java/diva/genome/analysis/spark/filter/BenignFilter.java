package diva.genome.analysis.spark.filter;

import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.ProteinVariantAnnotation;
import org.opencb.biodata.models.variant.avro.Score;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 26/02/2017.
 */
public class BenignFilter  implements Function<AllelesAvro, Boolean>, Predicate<AllelesAvro>,
        org.apache.spark.api.java.function.Function<AllelesAvro, Boolean> {

    public static final String SIFT = "sift";
    public static final String POLYPHEN = "polyphen";
    public static final String SIFT_TOLERATED = "tolerated";
    public static final String POLY_BENIGN = "benign";


    @Override
    public Boolean apply(AllelesAvro allelesAvro) {
        if (allelesAvro.getAnnotation() == null || allelesAvro.getAnnotation().getConsequenceTypes() == null) {
            return  false;
        }
        return hasBenign(allelesAvro.getAnnotation().getConsequenceTypes());
    }

    @Override
    public boolean test(AllelesAvro allelesAvro) {
        return apply(allelesAvro);
    }

    @Override
    public Boolean call(AllelesAvro allelesAvro) throws Exception {
        return apply(allelesAvro);
    }

    public boolean hasBenign(List<ConsequenceType> annotations) {
        if (null == annotations) {
            return false;
        }
        return annotations.stream().anyMatch(a -> isBenign(a.getProteinVariantAnnotation()));
    }

    public boolean isBenign(ProteinVariantAnnotation prot) {
        if (null == prot) {
            return false;
        }
        List<Score> scores = prot.getSubstitutionScores();
        if (null == scores) {
            return false;
        }
        boolean siftTolerated = false;
        boolean polyBenign = false;
        for (Score score : scores) {
            if (StringUtils.equals(score.getSource(), SIFT)
                    && StringUtils.equals(score.getDescription(), SIFT_TOLERATED)) {
                siftTolerated = true;
            } else if (StringUtils.equals(score.getSource(), POLYPHEN)
                    && StringUtils.equals(score.getDescription(), POLY_BENIGN)) {
                polyBenign = true;
            }
        }
        return siftTolerated && polyBenign;
    }

}
