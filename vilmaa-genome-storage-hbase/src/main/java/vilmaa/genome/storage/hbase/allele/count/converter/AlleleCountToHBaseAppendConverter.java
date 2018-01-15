package vilmaa.genome.storage.hbase.allele.count.converter;

import vilmaa.genome.storage.hbase.allele.count.AlleleCalculator;
import org.apache.hadoop.hbase.client.Append;

import java.util.Collection;

/**
 * Convert counted alleles to {@link Append} objects to store in HBase.
 * Created by mh719 on 23/02/2017.
 */
public interface AlleleCountToHBaseAppendConverter {
    Collection<Append> convert(String chromosome, AlleleCalculator calculator);
}
