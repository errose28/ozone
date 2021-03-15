/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.upgrade;

import org.apache.commons.codec.CharEncoding;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;

/**
 * Creates a MiniOzoneCluster in a pre-finalized state using
 * {@code HDDSLayoutFeature.INITIAL_VERSION}, and write data to it.
 * Make sure that the container file created does not write the
 * `schemaVersion` field, so it will be backwards compatible after a downgrade.
 */
public class TestContainerSchemaVersions {
  private static final int NUM_DATA_NODES = 1;

  private static final ReplicationFactor REP_FACTOR = ReplicationFactor.ONE;
  private static final ReplicationType REP_TYPE = ReplicationType.RATIS;

  private MiniOzoneCluster cluster;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    conf.set(OZONE_DATANODE_PIPELINE_LIMIT, "1");

    // Pre finalized cluster (Current code using older metadata layout version).
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATA_NODES)
        .setTotalPipelineNumLimit(NUM_DATA_NODES)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .setScmLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
        .setDnLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
        .build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSchemaVersionField() throws Exception {
    // When using old MLV, the new schema version field should not be
    // persisted to the container files.
    writeKey("old");
    assertReadKey("old");

    List<HddsDatanodeService> dnList = cluster.getHddsDatanodes();
    Assert.assertEquals(dnList.size(), NUM_DATA_NODES);

    for(HddsDatanodeService dn: dnList) {
      Iterator<Container<?>> contIter = dn.getDatanodeStateMachine()
          .getContainer().getContainerSet().getContainerIterator();

      Assert.assertTrue(contIter.hasNext());

      while(contIter.hasNext()) {
        Container<?> cont = contIter.next();
        assertSchemaVersion(cont, null);
      }
    }
  }

  // Pass null schema version to indicate it should not be in the file.
  private void assertSchemaVersion(Container<?> container,
     String schemaVersion) throws Exception {
    File contFile = container.getContainerFile();
    String fileContent = FileUtils.readFileToString(contFile,
        CharEncoding.UTF_8);

    if (schemaVersion != null) {
      String schemaField = OzoneConsts.SCHEMA_VERSION + ": " + schemaVersion;
      Assert.assertTrue(fileContent.contains(schemaField));
    } else {
      Assert.assertFalse(fileContent.contains(OzoneConsts.SCHEMA_VERSION));
    }
  }

  /**
   * Creates a volume, bucket and key all with name {@code content}, whose
   * data is also {@code content}.
   */
  private void writeKey(String content) throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    store.createVolume(content);
    store.getVolume(content).createBucket(content);
    OzoneOutputStream stream =
        store.getVolume(content).getBucket(content)
        .createKey(content, 10, REP_TYPE,
            REP_FACTOR, new HashMap<>());

    stream.write(content.getBytes(UTF_8));
    stream.close();
  }

  /**
   * Reads a volume, bucket and key all with the name {@code content},
   * and checks that the key's data matches {@code content}.
   */
  private void assertReadKey(String content) throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    OzoneInputStream stream =
        store.getVolume(content).getBucket(content).readKey(content);

    byte[] bytes = new byte[content.length()];
    int numRead = stream.read(bytes);
    Assert.assertEquals(content.length(), numRead);
    Assert.assertEquals(content, new String(bytes, UTF_8));
  }
}