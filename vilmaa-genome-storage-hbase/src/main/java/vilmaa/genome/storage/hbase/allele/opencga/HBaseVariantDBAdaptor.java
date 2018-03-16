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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.VariantHBaseResultSetIterator;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HBaseVariantDBAdaptor extends VariantHadoopDBAdaptor {

    private final HBaseVariantSourceDBAdaptor hBaseVariantSourceDBAdaptor;
    private final AlleleSqlQueryParser alleleQueryParser;

    public HBaseVariantDBAdaptor(HBaseCredentials credentials, StorageConfiguration configuration, Configuration conf) throws IOException {
        this(null, credentials, configuration, getHbaseConfiguration(conf, credentials));
    }

    public HBaseVariantDBAdaptor(Connection connection, HBaseCredentials credentials, StorageConfiguration
            configuration, Configuration conf) throws IOException {
        super(connection, credentials, configuration, conf);
        this.hBaseVariantSourceDBAdaptor = new HBaseVariantSourceDBAdaptor(getGenomeHelper());
        this. alleleQueryParser =
                new AlleleSqlQueryParser(getGenomeHelper(), this.getVariantTable(),
                        new VariantDBAdaptorUtils(this), isClientSideSkip());
    }

    @Override
    protected VariantHBaseResultSetIterator buildResultSetIterator(QueryOptions options, Statement statement, ResultSet resultSet, List<String> returnedSamples) throws SQLException {
        getLog().debug("Creating {} iterator", HBaseVariantResultSetIterator.class);
        HBaseVariantResultSetIterator iterator = new HBaseVariantResultSetIterator(statement,
                resultSet, getGenomeHelper(), getStudyConfigurationManager(), options, returnedSamples);
        return iterator;
    }

    @Override
    public HadoopVariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return this.hBaseVariantSourceDBAdaptor;
    }

    @Override
    protected VariantSqlQueryParser getQueryParser() {
        return this.alleleQueryParser;
    }

    @Override
    public VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        return super.get(query, options);
    }

}
