/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

// TODO :
// - Invoke CLI tool using
//    String[] args = new String[] {"ldb", "scan", "--column_family=default"};
//    RDBParser rdbParser = new RDBParser();
//    rdbParser.run(args);
// - Separate Json creation and table listings into from the methods that print them, so we can
//   read the return values of the methods instead of parsing stdout.
//    - May want to return JsonObject for json for easier comparison.
// - Need to determine how to read RocksDB from the MiniOzoneCluster.
//    - If not possible, will need to abandon MiniOzoneCluster and manually create RocksDB instance.
public class TestRDBParser {
  private static MiniOzoneCluster cluster;
  private static String volumeName;
  private static String bucketName;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static ObjectStore objectStore;
  private static OzoneClient client;

  @BeforeClass
  public static void init() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf).build();

    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "testrdbparser";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
