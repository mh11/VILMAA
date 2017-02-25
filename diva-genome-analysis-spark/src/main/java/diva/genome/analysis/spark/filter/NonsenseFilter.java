package diva.genome.analysis.spark.filter;

import diva.genome.analysis.VariantConsequence;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.ProteinVariantAnnotation;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 25/02/2017.
 */
public class NonsenseFilter implements Function<AllelesAvro, Boolean>, Predicate<AllelesAvro> ,
        org.apache.spark.api.java.function.Function<AllelesAvro, Boolean> {

    public static final String CADD_SCALED = "cadd_scaled";
    public static final String SIFT = "sift";
    public static final String POLYPHEN = "polyphen";
    public static final String SIFT_TOLERATED = "tolerated";
    public static final String POLY_BENIGN = "benign";
    public static final int CADD_CUTOFF = 15;
    public static final String BIOTYPE_PROTEIN_CODING = "protein_coding";

    @Override
    public boolean test(AllelesAvro allelesAvro) {
        VariantAnnotation annotation = allelesAvro.getAnnotation();
        if (null == annotation || null == annotation.getConsequenceTypes()) {
            return false;
        }
        if (!hasHighCadd(annotation.getFunctionalScore())){
            return false;// not a high cadd score
        }
        return annotation.getConsequenceTypes().stream().anyMatch(c -> valid(c));
    }

    public Collection<ConsequenceType> validConsequences(AllelesAvro allelesAvro) {
        VariantAnnotation annotation = allelesAvro.getAnnotation();
        if (null == annotation || null == annotation.getConsequenceTypes()) {
            return Collections.emptyList();
        }
        if (!hasHighCadd(annotation.getFunctionalScore())){
            return Collections.emptyList();// not a high cadd score
        }
        return annotation.getConsequenceTypes().stream().filter(c -> valid(c)).collect(Collectors.toList());
    }

    public boolean valid(ConsequenceType consequenceType) {
        if (null == consequenceType) {
            return false;
        }
        if (null == consequenceType.getSequenceOntologyTerms() || consequenceType.getSequenceOntologyTerms().isEmpty()) {
            return false;
        }
        if (!StringUtils.equals(BIOTYPE_PROTEIN_CODING, consequenceType.getBiotype())) {
            return false;
        }
        boolean high = consequenceType.getSequenceOntologyTerms().stream()
                .anyMatch(so -> VariantConsequence.isHigh(so.getName()));
        if (!high) {
            return false;
        }
        boolean benign = false;
        if (null != consequenceType.getProteinVariantAnnotation()) {
            benign = isBenign(consequenceType.getProteinVariantAnnotation());
        }
        if (benign) {
            return false;
        }
        return true;
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


    @Override
    public Boolean apply(AllelesAvro allelesAvro) {
        return test(allelesAvro);
    }

    @Override
    public Boolean call(AllelesAvro allelesAvro) throws Exception {
        return test(allelesAvro);
    }
}
