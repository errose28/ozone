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

  private String rocksDBPath;

  public DBColumnFamilySerializer(String rocksDBPath) {
    this.rocksDBPath = rocksDBPath;
  }

  @Override
  public JsonElement serialize(DBColumnFamilyDefinition dbDef, Type typeOfDBDef,
                               JsonSerializationContext context) throws IOException, RocksDBException {

    // Get the column family descriptor using its name.
    byte[] columnFamilyNameBytes = dbDef.getTableName().getBytes(StandardCharsets.UTF_8);
    ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(columnFamilyNameBytes);

    // Open only the desired column family in the RocksDB instance, and get a handle for it.
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    RocksDB rocksDB = RocksDB.openReadOnly(this.rocksDBPath, Arrays.asList(cfDescriptor), cfHandles);
    ColumnFamilyHandle cfHandle = cfHandles.get(0);

    // Serialize all key value pairs in the column family.
    RocksIterator iterator = rocksDB.newIterator(cfHandle);
    iterator.seekToFirst();

    JsonObject root = new JsonObject();
    JsonArray columnFamily = new JsonArray();

    while (iterator.isValid()) {
      Object key = null;
      Object value = null;

      key = dbDef.getValueCodec().fromPersistedFormat(iterator.value());
      value = dbDef.getKeyCodec().fromPersistedFormat(iterator.value());

      JsonObject keyValue = new JsonObject();
      keyValue.add("key", context.serialize(key));
      keyValue.add("value", context.serialize(value));

      columnFamily.add(keyValue);

      iterator.next();
    }

    root.add("column_family", columnFamily);

    // Cleanup: column family handle must be closed before database.
    cfHandle.close();
    rocksDB.close();

    return root;
  }
}

