/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilySerializer;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeDBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Debugging tool that allows the user to scan the contents of om, scm, or datanode rocksdb
 * instances and display their contents as json.
 */
@CommandLine.Command(
        name = "scan",
        description = "Parse specified metadataTable"
)
public class DBScanner implements Callable<Void> {

  @CommandLine.Option(names = {"--column_family"},
          description = "Column family to parse, or 'default' if unspecified",
          defaultValue = "default")
  private String tableName;

  @CommandLine.Option(names = {"--db_type"},
          description = "Type to parse database as (om, scm, datanode)",
          required = true)
  private String dbType;

  @CommandLine.ParentCommand
  private RDBParser parent;

  private static final String OM = "om";
  private static final String SCM = "scm";
  private static final String DATANODE = "datanode";

  @Override
  public Void call() {
    // Extract the database and column family from the CLI arguments.
    DBDefinition dbDef = parseDBType();
    DBColumnFamilyDefinition colFamilyDef = parseColFamily(dbDef);

    DBColumnFamilySerializer cfSerializer = new DBColumnFamilySerializer(parent.getDbPath());

    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.setPrettyPrinting();
    gsonBuilder.registerTypeAdapter(DBColumnFamilyDefinition.class, cfSerializer);
    Gson gson = gsonBuilder.create();

    String result = gson.toJson(colFamilyDef);
    System.out.println(result);

    return null;
  }

  /**
   * Determines the {@link DBDefinition} corresponding to the user's input in {@code dbType}.
   * If there is no corresponding {@link DBDefinition}, prints an error and exits.
   *
   * @return The {@link DBDefinition} corresponding to the user's input.
   */
  private DBDefinition parseDBType() {
    DBDefinition dbDef = null;

    if (dbType.equalsIgnoreCase(OM)) {
      dbDef = new OMDBDefinition();
    }
    else if (dbType.equalsIgnoreCase(SCM)) {
      dbDef = new SCMDBDefinition();
    }
    else if (dbType.equalsIgnoreCase(DATANODE)) {
      dbDef = new DatanodeDBDefinition(parent.getDbPath());
    }
    else {
      cliErrorExit("Unknown database type " + dbType + ". Options are " +
              String.join(" ", OM, SCM, DATANODE));
    }

    return dbDef;
  }

  /**
   * Returns the {@link DBColumnFamilyDefinition} corresponding to the user's input in {@code
   * tableName}. Prints an error and exits if {@code tableName} is not found in the RocksDB
   * database pointed to by {@code parent.DBPath()} or if it is not found in {@code dbDef}.
   *
   * @param dbDef The database definition which contains the list of column families.
   * @return The {@link DBColumnFamilyDefinition} in {@code dbDef} corresponding to the user's
   * input.
   */
  private DBColumnFamilyDefinition parseColFamily(DBDefinition dbDef) {
    // Find the desired column family in the database
    boolean colFamilyInDB = false;
    DBColumnFamilyDefinition colFamily = null;

    try {
      colFamilyInDB = RocksDB.listColumnFamilies(new Options(), parent.getDbPath())
              .stream()
              .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
              .anyMatch(cfName -> cfName.equals(tableName));

    }
    catch (RocksDBException e) {
      cliErrorExit("Failed to read column families from " + tableName + ": " + e.getMessage());
    }

    if (!colFamilyInDB){
        cliErrorExit("Column family " + tableName + " not found in " + parent.getDbPath());
    }
    else {
      // Find the desired column family in the DBDefinition.
      colFamily = Arrays.stream(dbDef.getColumnFamilies())
                      .filter(cfDef -> cfDef.getName().equals(tableName))
                      .findFirst()
                      .orElse(null);

      if (colFamily == null) {
        cliErrorExit("No codecs found to serialize column family " + tableName + " in " + parent.getDbPath());
      }
    }

    return colFamily;
  }

  /**
   * Prints a message to stderr (with a trailing newline) and exits.
   *
   * @param message The message to print to stderr before exiting.
   */
  private static void cliErrorExit(String message) {
    System.err.println(message);
    System.exit(1);
  }
}
