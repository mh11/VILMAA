package diva.genome.storage.hbase.allele.count.region;

import diva.genome.storage.hbase.allele.count.AlleleInfo;
import diva.genome.util.PointRegion;
import diva.genome.util.Region;
import diva.genome.util.RegionImpl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Store and collates {@link diva.genome.storage.hbase.allele.count.AlleleInfo} for a region in memory and allows specific queries.
 * Created by mh719 on 18/03/2017.
 */
public class AlleleRegionStore {
    private final Region targetRegion;
    private final List<Region<AlleleInfo>> reference = new ArrayList<>();
    private final List<Region<AlleleInfo>> noCall = new ArrayList<>();
    private final List<Region<AlleleInfo>> variation = new ArrayList<>();

    public AlleleRegionStore(Integer start, Integer endInclusive) {
        this.targetRegion = new RegionImpl(StringUtils.EMPTY, start, endInclusive);
    }

    public Region getTargetRegion() {
        return targetRegion;
    }

    public void addAll(Collection<Region<AlleleInfo>> regions) {
        regions.forEach(r -> add(r));
    }

    public void add(Region<AlleleInfo> region) {
        if (!region.overlap(this.targetRegion)) {
            return; // no overlap with target region
        }
        switch (region.getData().getId().length) {
            case 0:
                this.reference.add(region);
                break;
            case 1:
                this.noCall.add(region);
                break;
            default:
                this.variation.add(region);
                break;
        }
    }

    public void getReference(Consumer<Region<AlleleInfo>> consumer) {
        reference.stream().forEach(consumer);
    }

    public void getNocall(Consumer<Region<AlleleInfo>> consumer) {
        noCall.stream().forEach(consumer);
    }


    public void getVariation(Consumer<Region<AlleleInfo>> consumer) {
        variation.stream().forEach(consumer);
    }

    public void getReference(int position, Consumer<Region<AlleleInfo>> consumer) {
        getReference(new PointRegion(null, position), consumer);
    }

    public void getReference(Region target, Consumer<Region<AlleleInfo>> consumer) {
        reference.stream().filter(r -> r.overlap(target)).forEach(consumer);
    }

    public void getNocall(int position, Consumer<Region<AlleleInfo>> consumer) {
        getNocall(new PointRegion(null, position), consumer);
    }

    public void getNocall(Region target, Consumer<Region<AlleleInfo>> consumer) {
        noCall.stream().filter(r -> r.overlap(target)).forEach(consumer);
    }

    public void getVariation(int position, Consumer<Region<AlleleInfo>> consumer) {
        getVariation(new PointRegion(null, position), consumer);
    }

    public void getVariation(Region target, Consumer<Region<AlleleInfo>> consumer) {
        variation.stream().filter(r -> r.overlap(target)).forEach(consumer);
    }

    public void getInfos(int position, Consumer<Region<AlleleInfo>> consumer) {
        getVariation(position, consumer);
        getNocall(position, consumer);
        getReference(position, consumer);
    }
}
