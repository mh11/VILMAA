package diva.genome.storage.hbase.allele.count.converter;

import diva.genome.storage.hbase.allele.count.AlleleCalculator;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
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
