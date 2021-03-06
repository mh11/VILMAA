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

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import vilmaa.genome.storage.hbase.allele.count.converter.AlleleCountToHBaseCompactConverter;
import vilmaa.genome.storage.hbase.allele.count.converter.GroupedAlleleCountToHBaseAppendConverter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static vilmaa.genome.storage.hbase.VariantHbaseUtil.*;

/**
 * Created by mh719 on 09/02/2017.
 */
public class FromPhoenixToProtoMapper extends AbstractVariantTableMapReduce {
    private static final byte[] REF_COLUMN = {1};
    private static final byte[] ALT_COLUMN = {2};
    private byte[] studiesRow;
    protected HBaseToAlleleCountConverter converter;
    private GroupedAlleleCountToHBaseAppendConverter groupedConverter;

    protected volatile List<Pair<Variant, Result>> positionBuffer = new ArrayList<>();

    public void setGroupedConverter(GroupedAlleleCountToHBaseAppendConverter groupedConverter) {
        this.groupedConverter = groupedConverter;
    }

    public void setConverter(HBaseToAlleleCountConverter converter) {
        this.converter = converter;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.studiesRow = this.getHelper().generateVariantRowKey("_METADATA", 0);
        converter = new HBaseToAlleleCountConverter();
//        groupedConverter = new AlleleCountToHBaseAppendGroupedConverter(getHelper().getColumnFamily());
        byte[] columnFamily = getHelper().getColumnFamily();
        groupedConverter = new AlleleCountToHBaseCompactConverter(columnFamily, REF_COLUMN, ALT_COLUMN);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        getLog().info("Start setup ...");
        this.setup(context);
        getLog().info("Finished setup ...");
        String chromosome = "-1";
        int referencePosition = -1;
        Consumer<List<Append>> submitFunction = appends -> {
            getLog().info("Map {} appends to puts ... ", appends.size());
            List<Put> puts = appends.stream().map(a -> {
                Put put = new Put(a.getRow());
                put.setFamilyCellMap(a.getFamilyCellMap());
                return put;
            }).collect(Collectors.toList());
            context.getCounter("OPENCGA", "transfer-append").increment(1);
            puts.forEach(put -> {
                try {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
                } catch (IOException | InterruptedException e) {
                    throw new IllegalStateException("Issue with submitting put ...", e);
                }
            });
        };

        try {
            while (context.nextKeyValue()) {
                Result result = context.getCurrentValue();
                context.getCounter("OPENCGA", "Found ...").increment(1);
                if (isMetaRow(result.getRow())) {
                    context.getCounter("OPENCGA", "META_ROW").increment(1);
                    continue;
                }
                Variant variant = inferAndSetType(getHelper().extractVariantFromVariantRowKey(result.getRow()));
                int nextPos = groupedConverter.calculateGroupPosition(variant.getStart());
                if (referencePosition != nextPos) {
                    context.getCounter("OPENCGA", "FLUSH").increment(1);
                    getLog().info("Flush buffer for " + referencePosition + " before adding " + variant);
                    flushBuffer(chromosome, submitFunction);
                }
                chromosome = variant.getChromosome();
                referencePosition = groupedConverter.calculateGroupPosition(variant.getStart());
                context.getCounter("OPENCGA", "add-to-buffer").increment(1);
                addToBuffer(new ImmutablePair<>(variant, result));
            }
            // end
            flushBuffer(chromosome, submitFunction);
        } catch (Exception e) {
            throw new IllegalStateException("Something went wrong during transfer", e);
        } finally {
            this.cleanup(context);
        }
    }

    public void flushBuffer(String chromosome, Consumer<List<Append>> submitFunction) {
        Map<Integer, AlleleCountPosition> refMap = new HashMap<>();
        Map<Integer, Map<String, AlleleCountPosition>> altMap = new HashMap<>();

        this.positionBuffer.forEach(pair -> {
            Variant variant = pair.getLeft();
            Result result = pair.getRight();
            switch (variant.getType()) {
                case NO_VARIATION:
                    refMap.put(variant.getStart(), this.converter.convert(result));
                    break;
                case INDEL:
                case INSERTION:
                case DELETION:
                case SNV:
                case SNP:
                case MNV:
                case MNP:
                case MIXED:
                    Map<String, AlleleCountPosition> map = altMap.computeIfAbsent(variant.getStart(), x -> new HashMap<>());
                    AlleleCountPosition count = this.converter.convert(result);
                    String varId = variant.getReference() + "_" + variant.getAlternate();
                    map.put(varId, count);
                    break;
                default:
                    throw new IllegalStateException("Type not supported: " + variant.getType() + " for " + variant);
            }
        });

        Collection<Append> appends = groupedConverter.convert(chromosome, new DummyAlleleCalculator(refMap, altMap));
        this.positionBuffer.clear();
        if (null == appends || appends.isEmpty()) {
            return;
        }
        submitFunction.accept(new ArrayList<>(appends));
    }

    public void addToBuffer(Pair<Variant, Result> data) {
        this.positionBuffer.add(data);
    }

    protected boolean isMetaRow(byte[] rowKey) {
        return Bytes.startsWith(rowKey, this.studiesRow);
    }
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // don't use
    }
    @Override
    protected void doMap(VariantMapReduceContext variantMapReduceContext) throws IOException, InterruptedException {
        // do nothing
    }

}
