/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wenbao.flink.kudu;

import com.wenbao.flink.kudu.connector.KuduConnector;
import com.wenbao.flink.kudu.connector.KuduTableInfo;
import com.wenbao.flink.kudu.pojo.HbaseBeanFieldModel;
import com.wenbao.flink.kudu.serde.KuduSerialization;
import com.wenbao.flink.kudu.utils.KuduUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KuduSink<OUT> extends RichSinkFunction<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduSink.class);

    private static final long serialVersionUID = -893553371298868553L;

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;

    private KuduSerialization<OUT> serializer;

    private transient KuduConnector connector;

    private Class aClass;

    public KuduSink(String kuduMasters, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer) {
        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.serializer = serializer.withSchema(tableInfo.getSchema());
    }


    public KuduSink(String kuduMasters, KuduTableInfo tableInfo, Class aClass) {
        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.aClass = aClass;
    }

    public KuduSink<OUT> withEventualConsistency() {
        this.consistency = KuduConnector.Consistency.EVENTUAL;
        return this;
    }

    public KuduSink<OUT> withStrongConsistency() {
        this.consistency = KuduConnector.Consistency.STRONG;
        return this;
    }

    public KuduSink<OUT> withUpsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        return this;
    }

    public KuduSink<OUT> withInsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.INSERT;
        return this;
    }

    public KuduSink<OUT> withUpdateWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPDATE;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        if (connector != null) {
            return;
        }
        connector = new KuduConnector(kuduMasters, tableInfo, consistency, writeMode);
//        serializer.withSchema(tableInfo.getSchema());
    }


    @Override
    public void invoke(OUT value, Context context) throws Exception {
//        KuduRow kuduRow = serializer.serialize(value);
//        boolean response = connector.writeRow(kuduRow);
        HbaseBeanFieldModel[] hbaseBeanFieldModels = KuduUtils.getBeanFieldModels(aClass);
        boolean response = false;
        if (null != hbaseBeanFieldModels) {
            response = connector.writeRow(hbaseBeanFieldModels, value);
        }

        if (!response) {
            throw new IOException("error with some transaction");
        }
    }

    @Override
    public void close() throws Exception {
        if (this.connector == null) {
            return;
        }
        try {
            this.connector.close();
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }
}
