import vilmaa.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Created by mh719 on 25/02/2017.
 */
public class FirstSpark {

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
        SparkConf conf = buildSparkConf(home, "First test", master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<AllelesAvro> rdd = loadAvroFile(sc, avroPath);


        JavaRDD<AllelesAvro> cadd = rdd.filter(f ->
                nullSave(f.getAnnotation().getConsequenceTypes()).anyMatch(s ->
                        nullSave(s.getSequenceOntologyTerms()).anyMatch(o ->
                                o.getName().equals("missense_variant"))));
        long missenseCount = cadd.count();
        System.out.println("missenseCount = " + missenseCount);

        long totalCount = rdd.count();
        System.out.println("totalCount = " + totalCount);

//
//        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
//
//        // Creates a DataFrame from a specified file
//        Dataset<Row> df = spark.read().format("com.databricks.spark.avro").load("episodes.avro");
//        Encoders.
//
//// Saves the subset of the Avro records read in
//        df.filter(functions.expr("doctor > 5")).write()
//                .format("com.databricks.spark.avro")
//                .save("/tmp/output");
    }

    private static <T> Stream<T> nullSave(Collection<T> c) {
        if (null == c) {
            return Stream.empty();
        }
        return c.stream();
    }

    public static <T> JavaRDD<T> loadAvroFile(JavaSparkContext sc, String avroPath) {
        JavaPairRDD<AvroKey, NullWritable> records = sc.newAPIHadoopFile(avroPath, AvroKeyInputFormat.class, AvroKey.class, NullWritable.class, sc.hadoopConfiguration());
        return records.keys()
                .map(x -> (T) x.datum());
    }

    public static SparkConf buildSparkConf(String home, String name, String master) {
        SparkConf conf = new SparkConf()
                .setAppName(name);
        if (StringUtils.isNotEmpty(home)) {
            conf = conf.setSparkHome(home);
        }
        if (StringUtils.isNotEmpty(master)) {
            conf = conf.setMaster(master);
        }
        // https://www.cloudera.com/documentation/enterprise/5-6-x/topics/spark_avro.html#concept_hsz_nvn_45__section_u2b_kn5_st
//        conf.set("spark.sql.avro.compression.codec", "deflate")
//        conf.set("spark.sql.avro.deflate.level", "5")
        conf = conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        return conf;
    }

}
