package diva.genome.analysis.spark;

import diva.genome.analysis.spark.filter.NonsenseFilter;
import diva.genome.analysis.spark.filter.RareControlFilter;
import diva.genome.storage.models.alleles.avro.AlleleCount;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import diva.genome.storage.models.samples.avro.SampleCollection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static diva.genome.analysis.spark.SparkDiva.buildSparkConf;
import static diva.genome.analysis.spark.SparkDiva.loadAlles;

/**
 * Created by mh719 on 25/02/2017.
 */
public class HighImpactAnalysis {
    protected static final Logger LOG = LoggerFactory.getLogger(HighImpactAnalysis.class);

    public static Logger getLog() {
        return LOG;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String avroPath = StringUtils.EMPTY; // "file:///Users/mh719/data/to-avro_small.avro";
        String config = StringUtils.EMPTY;
        String master = StringUtils.EMPTY;
        String home = StringUtils.EMPTY; // "/usr/hdp/current/spark2-client/"
        if (args.length < 2) {
            throw new IllegalStateException("Path to file required");
        }
        avroPath = args[0];
        config = args[1];
        if (args.length > 2) {
            master =args[2];
        };
        if (args.length > 3) {
            home =args[3];
        };
        getLog().info("avroPath = {}", avroPath);
        getLog().info("config = {}", config);
        SampleCollection sConfig = loadSampleCollection(config);
        SampleCollectionSerializable seriConfig = new SampleCollectionSerializable(sConfig);

        SparkConf conf = buildSparkConf(home, HighImpactAnalysis.class.getName(), master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<AllelesAvro> rdd = loadAlles(sc, avroPath);
        PairFlatMapFunction<AllelesAvro, String, Set<Integer>> pfmf = alleles -> {
            Set<String> geneIds = new NonsenseFilter().validConsequences(alleles)
                    .stream().map(c -> c.getEnsemblGeneId()).collect(Collectors.toSet());
            Set<Integer> samples = withVariation(alleles);
            List<Tuple2<String, Set<Integer>>> ret = new ArrayList<>();
            geneIds.forEach(gid -> ret.add(new Tuple2<>(gid, samples)));
            return ret.iterator();
        };

        JavaRDD<AllelesAvro> remaining = rdd.filter(new DoFilter());
        JavaPairRDD<String, Set<Integer>> paired = remaining.flatMapToPair(pfmf);
        JavaPairRDD<String, Set<Integer>> groupedData = paired.reduceByKey((a, b) -> {
            Set<Integer> hs = new HashSet<>(a.size() + b.size());
            hs.addAll(a);
            hs.addAll(b);
            return hs;
        });
        groupedData.foreach(d -> printResult(d, seriConfig));
        getLog().info("Done");
    }


    public static class SampleCollectionSerializable implements Serializable, Externalizable {

        private volatile SampleCollection collection;

        public SampleCollectionSerializable() {

        }
        public SampleCollectionSerializable(SampleCollection collection) {
            this.collection = collection;
        }

        private static SampleCollection fromString(String s) {
            try {
                Schema schema = SampleCollection.getClassSchema();
                Decoder decoder = DecoderFactory.get().jsonDecoder(schema, s);
                SpecificDatumReader<SampleCollection> reader = new SpecificDatumReader<>
                        (SampleCollection.getClassSchema());
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private static String toString(SampleCollection collection) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()){
                DatumWriter<SampleCollection> writer = new GenericDatumWriter<>(SampleCollection.getClassSchema());
                JsonEncoder encoder = EncoderFactory.get().jsonEncoder(SampleCollection.getClassSchema(), out);
                writer.write(collection, encoder);
                encoder.flush();
                return out.toString();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(toString(this.collection));
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeObject(toString(this.collection));
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            collection = fromString(in.readObject().toString());
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            collection = fromString(in.readObject().toString());
        }
    }


    private static void printResult(Tuple2<String, Set<Integer>> d, SampleCollectionSerializable seriSample) {
        SampleCollection samples = seriSample.collection;
        Set<Integer> ids = d._2();
        String gene = d._1();
        getLog().info("Gene: {} with {} samples", gene, ids.size());
        samples.getCohorts().forEach((k, coll) -> {
            Set<String> sampleNames = coll.stream().filter(s -> ids.contains(s))
                    .map(s -> samples.getSamples().get(s)).collect(Collectors.toSet());
            getLog().info("  {}: {} has {}; [{}]", gene, k, sampleNames.size(), sampleNames);
        });
    }

    private static Set<Integer> withVariation(AllelesAvro alleles) {
        Set<Integer> samples = new HashSet<>();
        AlleleCount alleleCount = alleles.getAlleleCount();
        samples.addAll(alleleCount.getHet());
        samples.addAll(alleleCount.getHomVar());
        return samples;
    }

    private static SampleCollection loadSampleCollection(String config) throws IOException {
        Path path = new Path(config);
        try(FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(path);
            DataInputStream din = new DataInputStream(in)) {

            Schema schema = SampleCollection.getClassSchema();
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            SpecificDatumReader<SampleCollection> reader = new SpecificDatumReader<>
                    (SampleCollection.getClassSchema());
            return reader.read(null, decoder);
        }
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
