package diva.genome.storage.hbase;

import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;

import static org.opencb.biodata.models.variant.avro.VariantType.*;

/**
 * Created by mh719 on 26/05/2017.
 */
public class VariantHbaseUtil {


    public static Variant inferAndSetType (Variant var) {
        VariantType type = inferType(var.getReference(), var.getAlternate());
        var.setType(type);
        return var;
    }

    public static VariantType inferType (Variant var) {
        return inferType(var.getReference(), var.getAlternate());
    }

    public static boolean isInsertion(Variant var) {
        return isInsertion(var.getReference(), var.getAlternate(), var.getType());
    }

    public static boolean isInsertion(String ref, String alt, VariantType type) {
        if (INSERTION.equals(type)) {
            return true;
        }
        return INSERTION.equals(inferType(ref, alt));
    }

    /**
     * Infers the type from reference and alternate sequence.
     * Detects NO_VARIATION, SNV, MNV INSERTION, DELETION and MIXED. BUT does NOT set SV etc.
     *
     * @param ref Reference string (normalised)
     * @param alt Alternate string (normalised)
     * @return VariantType one of NO_VARIATION, SNV, MNV, INSERTION, DELETION, MIXED.
     * @throws IllegalStateException in case of unexpected combination
     * @throws UnsupportedOperationException in case of symbolic alleles e.g. <CNV> or [CNV] ...
     */
    public static VariantType inferType(String ref, String alt) throws UnsupportedOperationException, IllegalStateException {
        if (Allele.wouldBeSymbolicAllele(ref.getBytes()) && ref.length() > 1) {
            throw new UnsupportedOperationException("Symbolic ref alleles not supported!" + ref);
        }
        if (Allele.wouldBeSymbolicAllele(alt.getBytes()) && alt.length() > 1) {
            throw new UnsupportedOperationException("Symbolic alt alleles not supported!" + alt);
        }
        if (Allele.wouldBeNoCallAllele(ref.getBytes())) {
            ref = StringUtils.EMPTY;
        }
        if (Allele.wouldBeNoCallAllele(alt.getBytes())) {
            alt = StringUtils.EMPTY;
        }

        if (ref.length() == alt.length()) {
            if (ref.length() == 0) {
                return NO_VARIATION;
            } else if (ref.length() == 1) {
                return SNV;
            } else {
                return MNV;
            }
        }
        if (ref.length() == 0 && alt.length() > 0) {
            return INSERTION;
        }
        if (ref.length() > 0 && alt.length() == 0) {
            return DELETION;
        }
        if (ref.length() > 0 && alt.length() > 0) {
            return MIXED;
        }
        // Doesn't apply the SV cutoff of 50BP
        throw new IllegalStateException("Unkown variant type for " + ref + ":" + alt);
    }

}
