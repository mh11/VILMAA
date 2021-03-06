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

package vilmaa.genome.storage.hbase.allele.count.converter;

import com.google.protobuf.MessageLite;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.models.protobuf.AlleleMap;
import vilmaa.genome.storage.hbase.allele.models.protobuf.AlternateCount;
import vilmaa.genome.storage.hbase.allele.models.protobuf.ReferenceCountHBaseProto;
import vilmaa.genome.storage.hbase.allele.models.protobuf.SampleList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleCountPositionToAlleleCountHBaseProto {
    private volatile ByteArrayOutputStream bout;
    public AlleleCountPositionToAlleleCountHBaseProto() {
        this.bout = new ByteArrayOutputStream();
    }

    public byte[] toBytes(MessageLite msg) {
        bout.reset();
        try {
            msg.writeDelimitedTo(bout);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Problems during convertion from Proto to byte[]", e);
        }
    }

    public byte[] variantToBytes(AlleleCountPosition position) {
        return toBytes(toAlleleMap(position.getAlternate()));
    }

    public byte[] referenceToBytes(AlleleCountPosition position) {
        return toBytes(convert(position));
    }

    public boolean hasVariantData(AlleleCountPosition position) {
        return hasAnyData(position.getAlternate(), false);
    }

    public boolean hasReferenceData(AlleleCountPosition position) {
        if (!position.getNotPass().isEmpty()) {
            return true;
        }
        if (hasAnyData(position.getReference(), true)) {
            return true;
        }
        if (hasAnyAltData(position.getAltMap())) {
            return true;
        }
        return false;
    }

    private boolean hasAnyAltData(Map<String, Map<Integer, List<Integer>>> altMap) {
        return altMap.values().stream().anyMatch(e -> hasAnyData(e, false));
    }

    public boolean hasAnyData(Map<Integer, List<Integer>> map, boolean ignoreHomRef) {
        return map.entrySet().stream().anyMatch(entry -> {
            if (ignoreHomRef && entry.getKey().equals(2)) {
                return false;
            }
            return !entry.getValue().isEmpty();
        });
    }

    public ReferenceCountHBaseProto.Builder convertToBuilder(AlleleCountPosition position) {
        return ReferenceCountHBaseProto.newBuilder().setPassCount(position.getPass().size())
                .setHomRefCount(position.getReference().containsKey(2) ? position.getReference().get(2).size() : 0)
                .setNotPass(toSampleList(position.getNotPass()))
                .setReference(toAlleleMap(position.getReference(), true))
                .setAlternates(toAlternates(position.getAltMap()));
    }

    public ReferenceCountHBaseProto convert(AlleleCountPosition position) {
        return convertToBuilder(position).build();
    }

    private AlternateCount toAlternates(Map<String, Map<Integer, List<Integer>>> input) {
        AlternateCount.Builder builder = AlternateCount.newBuilder();
        builder.putAllAltMap(toAltMap(input));
        return builder.build();
    }

    private Map<String, AlleleMap> toAltMap(Map<String, Map<Integer, List<Integer>>> input) {
        Map<String, AlleleMap> map = new HashMap<>();
        input.forEach((k, val) -> map.put(k, toAlleleMap(val)));
        return map;
    }

    private AlleleMap toAlleleMap(Map<Integer, List<Integer>> map) {
        return toAlleleMap(map, false);
    }

    private AlleleMap toAlleleMap(Map<Integer, List<Integer>> map, boolean ignoreHomRef) {
        AlleleMap.Builder builder = AlleleMap.newBuilder();
        map.forEach((k, val) -> {
            if (ignoreHomRef && k.equals(2)) {
                return; // IGNORE HomRef
            }
            builder.putAlternate(k.toString(), toSampleList(val));
        });
        return builder.build();
    }

    public SampleList toSampleList(List<Integer> samples){
        Collections.sort(samples);
        return SampleList.newBuilder().addAllSampleIds(samples).build();
    }

}
