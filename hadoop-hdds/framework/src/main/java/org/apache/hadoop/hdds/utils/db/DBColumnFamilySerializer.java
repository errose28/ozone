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

public class DBColumnFamilySerializer implements JsonSerializer<DBColumnFamilyDefinition> {

  private final String rocksDBPath;

  public DBColumnFamilySerializer(String rocksDBPath) {
    this.rocksDBPath = rocksDBPath;
  }

  @Override
  public JsonElement serialize(DBColumnFamilyDefinition dbDef, Type typeOfDBDef,
                               JsonSerializationContext context) {

    // Get the column family descriptor using its name.
    String columnFamilyName = dbDef.getTableName();
    byte[] columnFamilyNameBytes = columnFamilyName.getBytes(StandardCharsets.UTF_8);
    ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(columnFamilyNameBytes);

    // Open only the desired column family in the RocksDB instance, and get a handle for it.
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();


    RocksDB rocksDB;
    try {
      rocksDB = RocksDB.openReadOnly(rocksDBPath, Arrays.asList(cfDescriptor), cfHandles);
    }
    catch (RocksDBException e) {
      String errorMsg = "Failed to open column family " + columnFamilyName + " in database " + rocksDBPath;
      throw new JsonParseException(errorMsg, e);
    }


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
        throw new JsonParseException("Failed to key value with codec.", e);
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

    root.add(columnFamilyName, columnFamily);

    // Cleanup: column family handle must be closed before database.
    cfHandle.close();
    rocksDB.close();

    return root;
  }
}

