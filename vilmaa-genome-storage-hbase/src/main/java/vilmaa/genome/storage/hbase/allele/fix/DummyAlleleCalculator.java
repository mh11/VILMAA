package vilmaa.genome.storage.hbase.allele.fix;

import vilmaa.genome.storage.hbase.allele.count.AlleleCalculator;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.opencb.biodata.models.variant.Variant;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Returns provided maps (test / backwards compatibility)
 * Created by mh719 on 18/03/2017.
 */
public class DummyAlleleCalculator implements AlleleCalculator {

    private final Map<Integer, AlleleCountPosition> refMap;
    private final Map<Integer, Map<String, AlleleCountPosition>> altMap;

    public DummyAlleleCalculator(Map<Integer, AlleleCountPosition> refMap, Map<Integer, Map<String, AlleleCountPosition>> altMap) {
        this.refMap = refMap;
        this.altMap = altMap;

    }

    @Override
    public void addVariant(Variant variant) {

    }

    @Override
    public Map<Integer, Map<String, AlleleCountPosition>> buildVariantMap() {
        return altMap;
    }

    @Override
    public Map<Integer, AlleleCountPosition> buildReferenceMap() {
        return refMap;
    }

    @Override
    public Set<Integer> getPass(Integer position) {
        return null;
    }

    @Override
    public Set<Integer> getNotPass(Integer position) {
        return null;
    }

    @Override
    public void onlyLeaveSparseRepresentation(int startPos, int nextStartPos, boolean removePass, boolean
            removeHomRef) {

    }

    @Override
    public void fillNoCalls(Collection<String> expectedSamples, long startPos, long nextStartPos) {

    }
}
