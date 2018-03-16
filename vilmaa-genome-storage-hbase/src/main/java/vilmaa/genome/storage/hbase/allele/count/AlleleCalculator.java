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

package vilmaa.genome.storage.hbase.allele.count;

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
