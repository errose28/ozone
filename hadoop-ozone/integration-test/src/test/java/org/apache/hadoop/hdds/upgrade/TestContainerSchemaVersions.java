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
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.TestSchemaOneBackwardsCompatibility;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.compiler.PluginProtos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.awt.image.BytePackedRaster;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;

/**
 * Tests that DataNode will throw an exception on creation when it reads in a
 * VERSION file indicating a metadata layout version larger than its
 * software layout version.
 */
public class TestContainerSchemaVersions {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHDDSUpgrade.class);
  private static final int NUM_DATA_NODES = 3;

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    conf.set(OZONE_DATANODE_PIPELINE_LIMIT, "1");
    conf.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, tempFolder.toString());
    cluster = null;
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSchemaVersionField() throws Exception {
    // Pre finalized cluster
    cluster = createNewCluster(HDDSLayoutFeature.INITIAL_VERSION);

    writeKey("old");
    assertReadKey("old");

    List<HddsDatanodeService> dnList = cluster.getHddsDatanodes();
    Assert.assertEquals(dnList.size(), NUM_DATA_NODES);

    for(HddsDatanodeService dn: dnList) {
      Iterator<Container<?>> contIter =
          dn.getDatanodeStateMachine().getContainer().getContainerSet().getContainerIterator();

      Assert.assertTrue(contIter.hasNext());

      while(contIter.hasNext()) {
        Container<?> cont = contIter.next();
        assertSchemaVersion(cont, null);
      }
    }

    // Create client
    // write data to container1
    // new cluster and client
    //    MiniOzoneCluster cluster = createNewCluster(HDDSLayoutFeature.DATANODE_SCHEMA_V2);
    // read data from container1
    // write data to container1
    // write data to container2


  }

  // Pass null schema version to indicate it should no be in the file.
  private void assertSchemaVersion(Container<?> container,
     String schemaVersion) throws Exception {
    File contFile = container.getContainerFile();
    String fileContent = FileUtils.readFileToString(contFile,
        CharEncoding.UTF_8);

    final String schemaField =
        OzoneConsts.SCHEMA_VERSION + ": " + schemaVersion;

    if (schemaVersion != null) {
      Assert.assertTrue(fileContent.contains(schemaField));
    } else {
      Assert.assertFalse(fileContent.contains(OzoneConsts.SCHEMA_VERSION));
    }
  }

  private void writeKey(String prefix) throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    store.createVolume(prefix + "vol");
    store.getVolume(prefix + "vol").createBucket(prefix + "bucket");
    OzoneOutputStream stream =
        store.getVolume(prefix + "vol").getBucket(prefix +
        "bucket")
        .createKey(prefix + "key", 10, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());

    stream.write(prefix.getBytes());
    stream.close();
  }

  private void assertReadKey(String prefix) throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    OzoneInputStream stream =
        store.getVolume(prefix + "vol").getBucket(prefix +
        "bucket")
        .readKey(prefix + "key");

    byte[] bytes = new byte[prefix.length()];
    int numRead = stream.read(bytes);
    Assert.assertEquals(prefix.length(), numRead);
    Assert.assertEquals(prefix, new String(bytes, UTF_8));
  }

  private MiniOzoneCluster createNewCluster(HDDSLayoutFeature layoutVersion)
      throws Exception {
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATA_NODES)
        // allow only one FACTOR THREE pipeline.
        .setTotalPipelineNumLimit(NUM_DATA_NODES + 1)
        .setHbInterval(1000)
        .setHbProcessorInterval(1000)
        .setScmLayoutVersion(layoutVersion.layoutVersion())
        .setDnLayoutVersion(layoutVersion.layoutVersion())
        .build();
    cluster.waitForClusterToBeReady();

    // Wait for the one ratis factor 3 pipeline.
    PipelineManager scmPipelineManager = cluster.getStorageContainerManager().getPipelineManager();
    LambdaTestUtils.await(10000, 2000, () -> {
      List<Pipeline> pipelines =
          scmPipelineManager.getPipelines(RATIS, THREE, OPEN);
      return pipelines.size() == 1;
    });

    return cluster;
  }
}