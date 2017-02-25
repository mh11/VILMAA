package diva.genome.analysis.spark.filter;

import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.opencb.biodata.models.variant.avro.VariantStats;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 25/02/2017.
 */
public class RareControlFilter implements Function<AllelesAvro, Boolean>, Predicate<AllelesAvro>,
        org.apache.spark.api.java.function.Function<AllelesAvro, Boolean> {
    public static final String CONTROL = "PAH_CONTROL";
    public static final String CASES = "PAH";

//    private final String cases;
//    private final String controls;

//    public RareControlFilter(String cases, String controls) {
//        this.cases = cases;
//        this.controls = controls;
//    }

    private boolean isInPah(Map<String, VariantStats> stats) {
        VariantStats pah = stats.get(CASES);
        if (null == pah) {
            return false; // no annotation -> no variation in this cohort seen.
        }
        return pah.getMaf() > 0;
    }

    private boolean isRareControl(Map<String, VariantStats> stats) {
        VariantStats ctrl = stats.get(CONTROL);
        if (null == ctrl) {
            return true; // no annotation -> no variation in this cohort seen.
        }
        return ctrl.getMaf() < 0.0001;
    }

    @Override
    public boolean test(AllelesAvro allelesAvro) {
        Map<String, VariantStats> stats = allelesAvro.getStats();
        if (null == stats || stats.isEmpty()) {
            return false;
        }
        return isRareControl(stats) && isInPah(stats);
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
