package diva.genome.analysis;

/**
 * Created by mh719 on 25/02/2017.
 */
public class VariantConsequence {

    public static Boolean isHigh(String name) {
        switch (name) {
            case "transcript_ablation":
            case "splice_acceptor_variant":
            case "splice_donor_variant":
            case "stop_gained":
            case "frameshift_variant":
            case "stop_lost":
            case "start_lost":
            case "transcript_amplification":
                return true;
            default:
                return false;
        }
    }
}
