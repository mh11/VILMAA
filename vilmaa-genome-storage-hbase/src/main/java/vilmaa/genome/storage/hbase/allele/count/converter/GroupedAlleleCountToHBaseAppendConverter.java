package vilmaa.genome.storage.hbase.allele.count.converter;

/**
 * Created by mh719 on 17/03/2017.
 */
public interface GroupedAlleleCountToHBaseAppendConverter extends AlleleCountToHBaseAppendConverter {

    Integer calculateGroupPosition(Integer position);

}
