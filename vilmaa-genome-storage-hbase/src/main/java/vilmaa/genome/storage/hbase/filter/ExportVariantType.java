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
import org.opencb.biodata.models.variant.avro.VariantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Filter variants by {@link VariantType}
 *
 * Created by mh719 on 05/01/2018.
 */
public class ExportVariantType implements IHbaseVariantFilter {
    public static final String CONFIG_ANALYSIS_EXPORT_TYPE_INCLUDE = "vilmaa.genome.storage.allele.type.include";
    public static final String CONFIG_ANALYSIS_EXPORT_TYPE_EXCLUDE = "vilmaa.genome.storage.allele.type.exclude";

    private Logger log = LoggerFactory.getLogger(this.getClass());

    protected volatile Set<VariantType> includes = new HashSet<>();
    protected volatile Set<VariantType> excludes = new HashSet<>();

    public static ExportVariantType build(Configuration conf) {
        ExportVariantType filter = new ExportVariantType();
        filter.addFilter(conf.getStrings(CONFIG_ANALYSIS_EXPORT_TYPE_INCLUDE, null), true);
        filter.addFilter(conf.getStrings(CONFIG_ANALYSIS_EXPORT_TYPE_EXCLUDE, null), false);
        return filter;
    }

    public ExportVariantType() {
        /* do nothing */
    }

    public void addFilter(String[] types, boolean isInclude) {
        if (Objects.isNull(types) || types.length == 0) {
            return; // do nothing
        }
        Set<VariantType> varTypes = translate(types);
        if (isInclude) {
            this.includes.addAll(varTypes);
        } else {
            this.excludes.addAll(varTypes);
        }
        getLog().info("Added {} types to {} ",varTypes, isInclude?"include":"exclude");
    }

    public Logger getLog() {
        return log;
    }

    private Set<VariantType> translate(String[] types) {
        return Arrays.stream(types).map(t -> VariantType.valueOf(t)).collect(Collectors.toSet());
    }

    @Override
    public boolean pass(Result value, Variant variant) {
        VariantType type = variant.getType();
        if (!includes.isEmpty()) {
            if (includes.contains(type)) return true; // if in INCLUDE list - OK
            return false;
        }
        if (!excludes.isEmpty()) {
            if (!excludes.contains(type)) return true; // if NOT in EXCLUDE - OK
            return false;
        }
        return false;  // IF NOT included and excluded
    }

    @Override
    public boolean hasFilters() {
        if (!includes.isEmpty()) return true;
        if (!excludes.isEmpty()) return true;
        return false;
    }
}
