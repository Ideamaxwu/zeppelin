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

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;

import java.util.Properties;

/**
 * Apache AsterixDB SQL++ Interpreter for Zeppelin.
 */
public class AsterixDBSQLPPInterpreter extends AsterixDBInterpreter {

  public AsterixDBSQLPPInterpreter(Properties property) {
    super(property);
  }

  static {
    Interpreter.register("sqlpp", "asterixdb", AsterixDBSQLPPInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add(ASTERIXDB_HOST, "localhost", "The host for AsterixDB HTTP API")
            .add(ASTERIXDB_PORT, "19002", "The port for AsterixDB HTTP API").build());
  }

  @Override
  protected String getResult(String query) {
    return api.executeSQLPP(query);
  }

}
