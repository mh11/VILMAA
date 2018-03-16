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
import vilmaa.genome.util.Region;

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
