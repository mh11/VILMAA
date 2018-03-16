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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.*;

/**
 * Bean to hold sample specific allele information
 * Created by mh719 on 17/03/2017.
 */
public class AlleleInfo {
    protected static final String[] NO_CALL_ALLELE = {"."};
    protected static final String[] REFERENCE_ALLELE = new String[0];

    private volatile boolean pass;
    private volatile int count;
    private volatile int depth;
    private volatile VariantType type;
    private volatile String[] id;
    private volatile Set<Integer> sampleIds;

    public AlleleInfo(int count, int depth) {
        this(count, depth, Collections.emptyList(), NO_CALL_ALLELE, VariantType.NO_VARIATION, false);
    }

    public AlleleInfo(int count, int depth, Collection<Integer> sampleIds, String[] id, VariantType type, boolean pass) {
        this.count = count;
        this.depth = depth;
        this.type = type;
        this.id = id;
        this.pass = pass;
        if (!Objects.isNull(sampleIds) && !sampleIds.isEmpty()) {
            this.sampleIds = new HashSet<>(sampleIds);
        } else {
            this.sampleIds = new HashSet<>();
        }
    }

    public AlleleInfo(int count, int depth, Integer sampleId, String[] id, VariantType type, boolean pass) {
        this(count, depth, null == sampleId ? Collections.emptyList() : Collections.singleton(sampleId), id, type, pass);
    }

    public AlleleInfo(AlleleInfo copy) {
        this(copy.getCount(), copy.getDepth(), copy.getSampleIds(), copy.getId(), copy.getType(), copy.isPass());
    }

    public void addSampleId(Integer sampleId) {
        this.sampleIds.add(sampleId);
    }
    public void setSampleIds(Collection<Integer> sampleIds) {
        this.sampleIds = new HashSet<>(sampleIds);
    }

    public Set<Integer> getSampleIds() {
        return sampleIds;
    }

    public void setPass(boolean pass) {
        this.pass = pass;
    }

    public boolean isPass() {
        return pass;
    }

    public int getCount() {
        return count;
    }

    public int getDepth() {
        return depth;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public void setType(VariantType type) {
        this.type = type;
    }

    public VariantType getType() {
        return type;
    }

    public void setId(String[] id) {
        this.id = id;
    }

    public String[] getId() {
        return id;
    }

    public String getIdString() {
        return buildVariantId(this.getId());
    }

    public static String buildVariantId(String[] refAlt) {
        return buildVariantId(StringUtils.EMPTY, refAlt);
    }

    public static String buildVariantId(String prefix, String[] refAlt) {
        if (refAlt.length == 0) {
            return StringUtils.EMPTY; // Reference
        }
        if (refAlt.length == 1) {
            return refAlt[0]; // NO_CALL
        }
        if (refAlt.length != 2) {
            throw new IllegalStateException("RefAlt array expected to be of length 2: " + Arrays.toString(refAlt));
        }
        return buildVariantId(prefix, refAlt[0], refAlt[1]);
    }

    public static String buildVariantId(String prefix, String ref, String alt) {
        return prefix + ref + "_" + alt;
    }

    public static String[] parseVariantId(String vid) {
        switch (vid.length()) {
            case 0: return REFERENCE_ALLELE;
            case 1: return NO_CALL_ALLELE;
            default:
                String[] arr = vid.split("_", 2);
                if (arr.length != 2) {
                    throw new IllegalStateException("Not supported: " + vid);
                }
                return arr;
        }
    }

    public static String[] getNoCallAllele() {
        return NO_CALL_ALLELE;
    }

    public static String[] getReferenceAllele() {
        return REFERENCE_ALLELE;
    }
}
