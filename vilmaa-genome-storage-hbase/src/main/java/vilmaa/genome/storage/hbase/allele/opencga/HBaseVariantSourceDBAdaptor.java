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

package vilmaa.genome.storage.hbase.allele.opencga;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HBaseVariantSourceDBAdaptor extends HadoopVariantSourceDBAdaptor {


    public HBaseVariantSourceDBAdaptor(Configuration configuration) {
        super(configuration);
    }

    public HBaseVariantSourceDBAdaptor(Connection connection, Configuration configuration) {
        super(connection, configuration);
    }

    public HBaseVariantSourceDBAdaptor(GenomeHelper genomeHelper) {
        super(genomeHelper);
    }
}
