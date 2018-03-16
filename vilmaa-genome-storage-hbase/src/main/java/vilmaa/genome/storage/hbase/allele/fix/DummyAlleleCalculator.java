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
