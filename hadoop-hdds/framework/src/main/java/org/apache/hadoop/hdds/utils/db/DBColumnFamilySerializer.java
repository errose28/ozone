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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import com.google.gson.*;
import org.rocksdb.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Custom json serializer to be used with gson to serialize {@link DBColumnFamilyDefinition}
 * objects.
 */
public class DBColumnFamilySerializer implements JsonSerializer<DBColumnFamilyDefinition> {

  private final String rocksDBPath;

  /**
   * Creates a new instance that can be used to serialize {@link DBColumnFamilyDefinition}s
   * belonging to the RocksDB database pointed to by {@param rocksDBPath}.
   *
   * @param rocksDBPath The path to the directory corresponding to the RocksDB database that this
   *                   serializer will read from.
   */
  public DBColumnFamilySerializer(String rocksDBPath) {
    this.rocksDBPath = rocksDBPath;
  }

  @Override
  public JsonElement serialize(DBColumnFamilyDefinition dbDef, Type typeOfDBDef,
                               JsonSerializationContext context) {

    // Create the required column family descriptors.
    String colFamilyName = dbDef.getTableName();
    ColumnFamilyDescriptor cfDescriptor = getColFamilyDescriptor(colFamilyName);
    ColumnFamilyDescriptor defaultDescriptor = getColFamilyDescriptor("default");

    // Populated if the database is successfully opened.
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    RocksDB rocksDB;
    try {
      DBOptions openOptions = new DBOptions();
      openOptions.setCreateIfMissing(false);
      openOptions.setCreateMissingColumnFamilies(false);

      // Rocks requires the default column family to always be passed to open,
      // even if we don't use it.
      rocksDB = RocksDB.openReadOnly(openOptions, rocksDBPath,
              Arrays.asList(cfDescriptor, defaultDescriptor), cfHandles);
    }
    catch (RocksDBException e) {
      // When gson encounters a JsonParseException, it will just display the message of the first
      // exception in the chain.
      // Therefore, add the message of the rocks exception for clarity.
      String errorMsg = e.getMessage() + "\nFailed to open column family " + colFamilyName +
              " in database " + rocksDBPath;
      throw new JsonParseException(errorMsg, e);
    }

    // First handle is the caller specified column family.
    ColumnFamilyHandle cfHandle = cfHandles.get(0);

    // Serialize all key value pairs in the column family.
    RocksIterator iterator = rocksDB.newIterator(cfHandle);
    iterator.seekToFirst();

    JsonObject root = new JsonObject();
    JsonArray columnFamily = new JsonArray();

    while (iterator.isValid()) {
      Object key;
      Object value;

      try {
        key = dbDef.getValueCodec().fromPersistedFormat(iterator.value());
      }
      catch (IOException e) {
        throw new JsonParseException("Failed to parse value with codec.", e);
      }

      try {
        value = dbDef.getKeyCodec().fromPersistedFormat(iterator.value());
      }
      catch (IOException e) {
        throw new JsonParseException("Failed to parse value with codec.", e);
      }

      JsonObject keyValue = new JsonObject();
      keyValue.add("key", context.serialize(key));
      keyValue.add("value", context.serialize(value));

      columnFamily.add(keyValue);

      iterator.next();
    }

    root.add(colFamilyName, columnFamily);

    // Cleanup: column family handle must be closed before database.
    cfHandle.close();
    rocksDB.close();

    return root;
  }

  /**
   * Helper method to create a column family descriptor from a String, instead of from a byte array.
   *
   * @param colFamilyName The name of the column family to create a
   * {@link ColumnFamilyDescriptor} for.
   * @return A new {@link ColumnFamilyDescriptor} created from {@code colFamilyName}.
   */
  private static ColumnFamilyDescriptor getColFamilyDescriptor(String colFamilyName) {
    byte[] columnFamilyNameBytes = colFamilyName.getBytes(StandardCharsets.UTF_8);
    return new ColumnFamilyDescriptor(columnFamilyNameBytes);
  }
}

