package vilmaa.genome.storage.hbase.filter;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;

/**
 * Created by mh719 on 07/12/2017.
 */
public interface IHbaseVariantFilter {
    String DEFAULT_CHROM = "other";
    String CHR_Y = "Y";
    String CHR_X = "X";

    boolean pass(Result value, Variant variant);

    boolean hasFilters();
}
