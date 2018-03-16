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
import vilmaa.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import org.apache.hadoop.hbase.client.Append;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by mh719 on 23/02/2017.
 */
public class AllelCountToHBaseSingleConverter implements AlleleCountToHBaseAppendConverter {
    private final AlleleCountToHBaseConverter converter;

    public AllelCountToHBaseSingleConverter(byte[] columnFamily, String studyId) {
        this.converter = new AlleleCountToHBaseConverter(columnFamily, studyId);
    }

    @Override
    public Collection<Append> convert(String chromosome, AlleleCalculator calculator) {
        Map<Integer, AlleleCountPosition> referenceMap = calculator.buildReferenceMap();
        Map<Integer, Map<String, AlleleCountPosition>> variantMap = calculator.buildVariantMap();
        List<Append> appends = new ArrayList<>();
        /* Convert Reference rows */
        referenceMap.forEach((position, count) -> {
                    Append convert = converter.convert(chromosome, position, count);
                    if (null != convert) {
                        appends.add(convert);
                    }
                }
        );
        /* Convert Variant rows */
        variantMap.forEach((position, entry) -> entry.forEach((alt, count) -> {
            appends.add(converter.convert(chromosome, position, alt, count));
        }));
        return appends;
    }
}
