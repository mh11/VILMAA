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
