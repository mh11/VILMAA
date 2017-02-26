package diva.genome.analysis.spark.filter;

import diva.genome.analysis.VariantConsequence;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import diva.genome.storage.models.alleles.avro.VariantAnnotation;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.avro.ConsequenceType;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 25/02/2017.
 */
public class NonsenseFilter implements Function<AllelesAvro, Boolean>, Predicate<AllelesAvro> ,
        org.apache.spark.api.java.function.Function<AllelesAvro, Boolean> {

    public static final String BIOTYPE_PROTEIN_CODING = "protein_coding";

    @Override
    public boolean test(AllelesAvro allelesAvro) {
        VariantAnnotation annotation = allelesAvro.getAnnotation();
        if (null == annotation || null == annotation.getConsequenceTypes()) {
            return false;
        }
        return annotation.getConsequenceTypes().stream().anyMatch(c -> valid(c));
    }

    public Collection<ConsequenceType> validConsequences(AllelesAvro allelesAvro) {
        VariantAnnotation annotation = allelesAvro.getAnnotation();
        if (null == annotation || null == annotation.getConsequenceTypes()) {
            return Collections.emptyList();
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
        return consequenceType.getSequenceOntologyTerms().stream()
                .anyMatch(so -> VariantConsequence.isHigh(so.getName()));
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
