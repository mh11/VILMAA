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

package vilmaa.genome.storage.hbase.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mh719 on 07/12/2017.
 */
public class ExportFilters implements IHbaseVariantFilter {
    private final List<IHbaseVariantFilter> filters = new ArrayList<>();

    public ExportFilters() {
        // empty
    }

    public static ExportFilters build(StudyConfiguration sc, Configuration conf, byte[] columnFamily) {
        ExportFilters filters = new ExportFilters();
        filters.addFilter(ExportPosRefAltFilter.build(conf));
        filters.addFilter(ExportOprFilter.build(sc, conf, columnFamily));
        filters.addFilter(ExportMafFilter.build(sc, conf, columnFamily));
        filters.addFilter(ExportVariantType.build(conf));
        return filters;
    }

    public void addFilter(IHbaseVariantFilter filter) {
        if (filter.hasFilters()) {
            filters.add(filter);
        }
    }

    @Override
    public boolean pass(Result value, Variant variant) {
        for (IHbaseVariantFilter filter : filters) {
            if (!filter.pass(value, variant)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasFilters() {
        return !filters.isEmpty();
    }

}
