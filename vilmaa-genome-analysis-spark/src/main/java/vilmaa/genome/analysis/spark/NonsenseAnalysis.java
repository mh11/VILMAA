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

package vilmaa.genome.analysis.spark;

import vilmaa.genome.storage.models.alleles.avro.AlleleCount;
import vilmaa.genome.storage.models.alleles.avro.AllelesAvro;
import vilmaa.genome.storage.models.samples.avro.SampleCollection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 25/02/2017.
 */
public class NonsenseAnalysis {
    protected static final Logger LOG = LoggerFactory.getLogger(NonsenseAnalysis.class);

    public static Logger getLog() {
        return LOG;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            throw new IllegalStateException("Path to file required");
        }
        String avroPath = args[0];
        String config = args[1];
        String user = args[2];
        getLog().info("avroPath = {}", avroPath);
        getLog().info("config = {}", config);
        SampleCollection sConfig = loadSampleCollection(config);
        SampleCollectionSerializable seriConfig = new SampleCollectionSerializable(sConfig);

        SparkConf sparkConf = SparkDiva.buildSparkConf(StringUtils.EMPTY, NonsenseAnalysis.class.getName(), StringUtils.EMPTY);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(TableInputFormat.INPUT_TABLE, avroPath);
        getLog().info("Using zookeeper parent {} ", hbaseConf.get("zookeeper.znode.parent"));
        hbaseConf.addResource(new Path("./hbase-site.xml"));
        getLog().info("Using zookeeper parent {} ", hbaseConf.get("zookeeper.znode.parent"));

        // http://stackoverflow.com/questions/38506755/connect-kerberos-secure-hbase-from-spark-streaming
        // http://stackoverflow.com/questions/34616676/should-i-call-ugi-checktgtandreloginfromkeytab-before-every-action-on-hadoop
        UserGroupInformation.getLoginUser().reloginFromKeytab();

        // http://stackoverflow.com/questions/35332026/issue-scala-code-in-spark-shell-to-retrieve-data-from-hbase
        Credentials creds = SparkHadoopUtil.get().getCurrentUserCredentials();
        UserGroupInformation cur = UserGroupInformation.getCurrentUser();
        cur.addCredentials(creds);
        cur.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.PROXY);

        // https://community.hortonworks.com/questions/46500/spark-cant-connect-to-hbase-using-kerberos-in-clus.html
//        hbaseConf.addResource(new Path(hbaseConfDir,"hbase-site.xml"));
//        hbaseConf.addResource(new Path(hadoopConfDir,"core-site.xml"));
//        hbaseConf.set("hbase.client.keyvalue.maxsize", "0");
//        hbaseConf.set("hbase.rpc.controllerfactory.class","org.apache.hadoop.hbase.ipc.RpcControllerFactory");
//        hbaseConf.set("hadoop.security.authentication", "kerberos");
//        hbaseConf.set("hbase.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(hbaseConf);
//        String keyTab="/etc/security/keytabs/somekeytab";
        String keyTab="./somekeytab";
        getLog().info("Set keytab ");
        UserGroupInformation ugi=UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keyTab);
        getLog().info("Set ugi ");
        UserGroupInformation.setLoginUser(ugi);
        getLog().info("Do execute ");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            getLog().info("Try to connect ");
            Connection connection = ConnectionFactory.createConnection(hbaseConf);
            getLog().info("Got connection {} ", connection);

            // http://stackoverflow.com/questions/25040709/how-to-read-from-hbase-using-spark
            // https://hortonworks.com/blog/spark-hbase-dataframe-based-hbase-connector/
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,
                    ImmutableBytesWritable.class, Result.class);

            getLog().info("Build RDD ...");
            long count = hbaseRdd.count();
            getLog().info("Count finished with {} entries", count);

            return null;
        });

//        JavaRDD<AllelesAvro> rdd = loadParquet(sc, avroPath);
//        PairFlatMapFunction<AllelesAvro, String, Set<Integer>> pfmf = alleles -> {
//            Set<String> geneIds = new NonsenseFilter().validConsequences(alleles)
//                    .stream().map(c -> c.getEnsemblGeneId()).collect(Collectors.toSet());
//            Set<Integer> samples = withVariation(alleles);
//            List<Tuple2<String, Set<Integer>>> ret = new ArrayList<>();
//            geneIds.forEach(gid -> ret.add(new Tuple2<>(gid, samples)));
//            return ret.iterator();
//        };
//
//        JavaRDD<AllelesAvro> remaining = rdd.filter(new DoFilter());
//        JavaPairRDD<String, Set<Integer>> paired = remaining.flatMapToPair(pfmf);
//        JavaPairRDD<String, Set<Integer>> groupedData = paired.reduceByKey((a, b) -> {
//            Set<Integer> hs = new HashSet<>(a.size() + b.size());
//            hs.addAll(a);
//            hs.addAll(b);
//            return hs;
//        });
//        groupedData.foreach(d -> printResult(d, seriConfig));
        getLog().info("Done");
    }

//    static class DoFilter implements Function<AllelesAvro, Boolean> {
//        private static NonsenseFilter nonsense = new NonsenseFilter();
//        private static RareControlFilter rare = new RareControlFilter();
//
//        @Override
//        public Boolean call(AllelesAvro allelesAvro) throws Exception {
//            return rare.call(allelesAvro) && nonsense.call(allelesAvro);
//        }
//    }

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


}
