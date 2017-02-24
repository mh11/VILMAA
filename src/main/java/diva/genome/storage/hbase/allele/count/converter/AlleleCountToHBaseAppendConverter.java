package diva.genome.storage.hbase.allele.count.converter;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.apache.hadoop.hbase.client.Append;

import java.util.Collection;
import java.util.Map;

/**
 * Created by mh719 on 23/02/2017.
 */
public interface AlleleCountToHBaseAppendConverter {
    Collection<Append> convert(String chromosome, Map<Integer, AlleleCountPosition> referenceMap, Map<Integer,
            Map<String, AlleleCountPosition>> variantMap);
}
