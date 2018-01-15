package vilmaa.genome.analysis.models.variant.stats;

/**
 * Created by mh719 on 28/03/2017.
 */
public class VariantStatistics extends org.opencb.biodata.models.variant.stats.VariantStats{

    private volatile Float overallPassRate;
    private volatile Float callRate;
    private volatile Float passRate;

    public VariantStatistics() {
        super();
    }

    public VariantStatistics(org.opencb.biodata.models.variant.stats.VariantStats stats) {
        super(stats.getImpl());
    }

    public Float getOverallPassRate() {
        return overallPassRate;
    }

    public void setOverallPassRate(Float overallPassRate) {
        this.overallPassRate = overallPassRate;
    }

    public Float getCallRate() {
        return callRate;
    }

    public void setCallRate(Float callRate) {
        this.callRate = callRate;
    }

    public Float getPassRate() {
        return passRate;
    }

    public void setPassRate(Float passRate) {
        this.passRate = passRate;
    }

}
