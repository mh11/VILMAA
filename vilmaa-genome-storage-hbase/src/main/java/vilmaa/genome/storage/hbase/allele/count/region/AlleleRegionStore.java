/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package vilmaa.genome.storage.hbase.allele.count.region;

import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.util.PointRegion;
import vilmaa.genome.util.Region;
import vilmaa.genome.util.RegionImpl;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Store and collates {@link AlleleInfo} for a region in memory and allows specific queries.
 * Created by mh719 on 18/03/2017.
 */
public class AlleleRegionStore {
    private final Region targetRegion;
    private final List<Region<AlleleInfo>> reference = new ArrayList<>();
    private final List<Region<AlleleInfo>> noCall = new ArrayList<>();
    private final List<Region<AlleleInfo>> variation = new ArrayList<>();

    public AlleleRegionStore(Integer start, Integer endInclusive) {
        this(new RegionImpl(StringUtils.EMPTY, start, endInclusive));
    }

    public AlleleRegionStore(Region targetRegion) {
        this.targetRegion = targetRegion;
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

    public void getAll(int position, Consumer<Region<AlleleInfo>> consumer) {
        getAll(new PointRegion(null, position), consumer);
    }

    public void getAll(Region region, Consumer<Region<AlleleInfo>> consumer) {
        getReference(region, consumer);
        getNocall(region, consumer);
        getVariation(region, consumer);
    }

    public void getAll(Consumer<Region<AlleleInfo>> consumer) {
        getReference(consumer);
        getNocall(consumer);
        getVariation(consumer);
    }

    public List<Region<AlleleInfo>> getAll(Region target) {
        List<Region<AlleleInfo>> lst = new ArrayList<>();
        getAll(target, v -> lst.add(v));
        return lst;
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

    public List<Region<AlleleInfo>> getReference(Region target) {
        List<Region<AlleleInfo>> lst = new ArrayList<>();
        getReference(target, v -> lst.add(v));
        return lst;
    }

    public void getNocall(int position, Consumer<Region<AlleleInfo>> consumer) {
        getNocall(new PointRegion(null, position), consumer);
    }

    public void getNocall(Region target, Consumer<Region<AlleleInfo>> consumer) {
        noCall.stream().filter(r -> r.overlap(target)).forEach(consumer);
    }

    public List<Region<AlleleInfo>> getNocall(Region target) {
        List<Region<AlleleInfo>> lst = new ArrayList<>();
        getNocall(target, v -> lst.add(v));
        return lst;
    }

    public void getVariation(int position, Consumer<Region<AlleleInfo>> consumer) {
        getVariation(new PointRegion(null, position), consumer);
    }

    public void getVariation(Region target, Consumer<Region<AlleleInfo>> consumer) {
        variation.stream().filter(r -> r.overlap(target, r.getData().getType().equals(VariantType.INSERTION)))
                .forEach(consumer);
    }

    public List<Region<AlleleInfo>> getVariation(Region target) {
        List<Region<AlleleInfo>> lst = new ArrayList<>();
        getVariation(target, v -> lst.add(v));
        return lst;
    }

    public void getInfos(int position, Consumer<Region<AlleleInfo>> consumer) {
        getVariation(position, consumer);
        getNocall(position, consumer);
        getReference(position, consumer);
    }
}
