package diva.genome.storage.hbase.allele.count.region;

import diva.genome.storage.hbase.allele.count.AlleleInfo;
import diva.genome.util.Region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Grouped {@link AlleleInfo} regions
 * Created by mh719 on 17/03/2017.
 */
public class PositionInfo {
    private final Map<Integer, List<Region<AlleleInfo>>> sampleAlleleRegionInfos = new HashMap<>();

    public PositionInfo() {
    }

    public void addInfo(Integer sampleId, Region<AlleleInfo> region) {
        this.sampleAlleleRegionInfos.computeIfAbsent(sampleId, (k) -> new ArrayList<>()).add(region);
    }

    public Map<Integer, List<Region<AlleleInfo>>> getSampleAlleleRegionInfos() {
        return sampleAlleleRegionInfos;
    }
}
