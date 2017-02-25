package diva.genome.analysis.spark;

import diva.genome.analysis.spark.filter.NonsenseFilter;
import diva.genome.analysis.spark.filter.RareControlFilter;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import static diva.genome.analysis.spark.SparkDiva.buildSparkConf;
import static diva.genome.analysis.spark.SparkDiva.loadAlles;

/**
 * Created by mh719 on 25/02/2017.
 */
public class HighImpactAnalysis {

    public static void main(String[] args) {
        String avroPath = StringUtils.EMPTY; // "file:///Users/mh719/data/to-avro_small.avro";
        String master = StringUtils.EMPTY;
        String home = StringUtils.EMPTY; // "/usr/hdp/current/spark2-client/"
        if (args.length < 1) {
            throw new IllegalStateException("Path to file required");
        }
        avroPath = args[0];
        if (args.length > 1) {
            master =args[1];
        };
        if (args.length > 2) {
            home =args[2];
        };
        System.out.println("avroPath = " + avroPath);
        SparkConf conf = buildSparkConf(home, HighImpactAnalysis.class.getName(), master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<AllelesAvro> rdd = loadAlles(sc, avroPath);


        JavaRDD<AllelesAvro> remaining = rdd.filter(new DoFilter());
        long count = remaining.count();
        System.out.println("count = " + count);
    }

    static class DoFilter implements Function<AllelesAvro, Boolean> {
        private static NonsenseFilter nonsense = new NonsenseFilter();
        private static RareControlFilter rare = new RareControlFilter();

        @Override
        public Boolean call(AllelesAvro allelesAvro) throws Exception {
            return rare.call(allelesAvro) && nonsense.call(allelesAvro);
        }
    }

}
