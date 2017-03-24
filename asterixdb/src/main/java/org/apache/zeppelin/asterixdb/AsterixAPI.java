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

package org.apache.zeppelin.asterixdb;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * AsterixDB API accessor
 */
public class AsterixAPI {
  private static final String AQL_PATH = "aql";
  private static final String SQLPP_PATH = "sqlpp";
  private static Logger logger = LoggerFactory.getLogger(AsterixAPI.class);
  private final HttpPost aqlHttpPost;
  private final HttpPost sqlppHttpPost;

  private final String aqlHost;
  private final String sqlppHost;

  public AsterixAPI(final String host, final String port) {
    this.aqlHost = "http://" +  host + ":" + port + "/" + AQL_PATH;
    this.sqlppHost = "http://" + host + ":" + port + "/" + SQLPP_PATH;
    this.aqlHttpPost = new HttpPost(aqlHost);
    this.sqlppHttpPost = new HttpPost(sqlppHost);
  }

  public String executeAQL(String aql) {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    String result = "";
    aqlHttpPost.setEntity(new StringEntity(aql, "UTF-8"));
    try {
      InputStream inputStream = httpclient.execute(aqlHttpPost).getEntity().getContent();
      StringWriter writer = new StringWriter();
      IOUtils.copy(inputStream, writer, "UTF-8");
      result = writer.toString();
    } catch (IOException e) {
      logger.error("Couldn't connect to AsterixDB HTTP API", e);
    }
    return result;
  }

  public String executeSQLPP(String sql) {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    String result = "";
    sqlppHttpPost.setEntity(new StringEntity(sql, "UTF-8"));
    try {
      InputStream inputStream = httpclient.execute(sqlppHttpPost).getEntity().getContent();
      StringWriter writer = new StringWriter();
      IOUtils.copy(inputStream, writer, "UTF-8");
      result = writer.toString();
    } catch (IOException e) {
      logger.error("Couldn't connect to AsterixDB HTTP API", e);
    }
    return result;
  }

}
