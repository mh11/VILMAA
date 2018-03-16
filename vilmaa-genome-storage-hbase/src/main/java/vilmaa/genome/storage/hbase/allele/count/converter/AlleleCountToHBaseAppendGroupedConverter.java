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

import vilmaa.genome.storage.hbase.allele.count.AlleleCalculator;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.apache.commons.math.exception.OutOfRangeException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleCountToHBaseAppendGroupedConverter implements GroupedAlleleCountToHBaseAppendConverter {

    public static final char PREFIX_REFERENCE = 'Y';
    public static final char PREFIX_VARIANT = 'Z';
    private String refPrefix;
    private String varPrefix;
    private final byte[] columnFamily;
    AlleleCountPositionToAlleleCountHBaseProto protoConverter = new AlleleCountPositionToAlleleCountHBaseProto();
    private int factor;

    public AlleleCountToHBaseAppendGroupedConverter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
        this.refPrefix = PREFIX_REFERENCE + "";
        this.varPrefix = PREFIX_VARIANT + "";
        this.factor = 100;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    @Override
    public Collection<Append> convert(String chromosome, AlleleCalculator calculator) {
        Map<Integer, AlleleCountPosition> referenceMap = calculator.buildReferenceMap();
        Map<Integer, Map<String, AlleleCountPosition>> variantMap = calculator.buildVariantMap();
        Map<Integer, Map<Integer, AlleleCountPosition>> regionMap = new HashMap<>();
        Map<Integer, Map<Integer, Map<String, AlleleCountPosition>>> regionVarMap = new HashMap<>();
        // group by position
        referenceMap.forEach((k, v) -> regionMap.computeIfAbsent(calculateGroupPosition(k), x -> new HashMap<>()).put(calcRemaining(k), v));
        variantMap.forEach((k, v) -> regionVarMap.computeIfAbsent(calculateGroupPosition(k), x -> new HashMap<>()).put(calcRemaining(k), v));

        Set<Integer> allPositions = new HashSet<>();
        allPositions.addAll(regionMap.keySet());
        allPositions.addAll(regionVarMap.keySet());

        List<Append> appends = new ArrayList<>();
        allPositions.forEach(refPos -> {
            AtomicBoolean send = new AtomicBoolean(false);
            Append append = new Append(buildRowKey(chromosome, refPos));
            addReferenceData(regionMap.get(refPos), send, append);
            addVariantData(regionVarMap.get(refPos), send, append);
            if (send.get()) {
                appends.add(append);
            }
        });
        return appends;
    }

    private void addVariantData(Map<Integer, Map<String, AlleleCountPosition>> altMap, AtomicBoolean send,
                                Append append) {
        if (null == altMap || altMap.isEmpty()) {
            return;
        }
        altMap.forEach((sub, map) -> map.forEach((varId, count) -> {
            if (protoConverter.hasVariantData(count)) {
                send.set(true);
                addVariantData(sub, varId, count, append);
            }
        }));
    }

    private void addVariantData(Integer prefix, String varId, AlleleCountPosition data, Append append) {
        byte[] bytes = protoConverter.variantToBytes(data);
        byte[] col = buildVariantColumn(prefix, varId);
        append.add(getColumnFamily(), col, bytes);
    }


    private void addReferenceData(Map<Integer, AlleleCountPosition> refMap, AtomicBoolean send, Append append) {
        if (null == refMap) {
            return;
        }
        refMap.forEach((prefix, count) -> {
            if (protoConverter.hasReferenceData(count)) {
                send.set(true);
                byte[] bytes = protoConverter.referenceToBytes(count);
                byte[] col = buildReferenceColumn(prefix);
                append.add(getColumnFamily(), col, bytes);
            }
        });
    }

    private byte[] buildVariantColumn(Integer col, String varId) {
        return Bytes.toBytes(this.varPrefix + col.toString() + ":" + varId);
    }

    private byte[] buildReferenceColumn(Integer col) {
        return Bytes.toBytes(this.refPrefix + col.toString());
    }

    private byte[] buildRowKey(String chromosome, Integer position) {
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(Integer.valueOf(chromosome.length())).intValue() +
                QueryConstants.SEPARATOR_BYTE_ARRAY.length + PUnsignedInt.INSTANCE.estimateByteSize(Integer.valueOf(position));
        byte[] rk = new byte[size];
        byte offset = 0;
        int var7 = offset + PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
        rk[var7++] = 0;
        PUnsignedInt.INSTANCE.toBytes(Integer.valueOf(position), rk, var7);
        return rk;
    }


    public Integer calcRemaining(Integer k) {
        int i = k % this.factor;
        if (i >= this.factor || i < 0) {
            throw new OutOfRangeException(i, 0, this.factor - 1);
        }
        return i;
    }

    @Override
    public Integer calculateGroupPosition(Integer position) {
        return position / factor;
    }
}
