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

import vilmaa.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by mh719 on 25/02/2017.
 */
public class SparkDiva {

    public static JavaRDD<AllelesAvro> loadParquet(JavaSparkContext sc, String avroPath) {
        JavaPairRDD<Void, AllelesAvro> pairRDD = sc.newAPIHadoopFile(avroPath, ParquetInputFormat
                .class, Void.class, AllelesAvro.class, sc.hadoopConfiguration());
        return pairRDD.values();
    }

    public static JavaRDD<AllelesAvro> loadAlles(JavaSparkContext sc, String avroPath) {
        return loadAvroFile(sc, avroPath);
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
