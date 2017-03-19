package diva.genome.storage.hbase.allele.count;

import org.opencb.biodata.models.variant.Variant;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Extract allele counts from a Variant
 * Created by mh719 on 17/03/2017.
 */
public interface AlleleCalculator {
    void addVariant(Variant variant);

    Map<Integer,Map<String,AlleleCountPosition>> buildVariantMap();

    Map<Integer, AlleleCountPosition> buildReferenceMap();

    Set<Integer> getPass(Integer position);

    Set<Integer> getNotPass(Integer position);

    void onlyLeaveSparseRepresentation(int startPos, int nextStartPos, boolean removePass, boolean removeHomRef);

    void fillNoCalls(Collection<String> expectedSamples, long startPos, long nextStartPos);



}
