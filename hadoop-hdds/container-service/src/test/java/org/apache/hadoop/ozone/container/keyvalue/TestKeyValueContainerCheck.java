/*
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

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;

import java.io.File;
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Basic sanity test for the KeyValueContainerCheck class.
 */
public class TestKeyValueContainerCheck
    extends TestKeyValueContainerIntegrityChecks {

  /**
   * Sanity test, when there are no corruptions induced.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testKeyValueContainerCheckNoCorruption(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    long containerID = 101;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration c = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks, true);
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);

    // first run checks on a Open Container
    boolean valid = kvCheck.fastCheck().isHealthy();
    assertTrue(valid);

    container.close();

    // next run checks on a Closed Container
    valid = kvCheck.fullCheck(new DataTransferThrottler(
        c.getBandwidthPerVolume()), null).isHealthy();
    assertTrue(valid);
  }

  /**
   * Sanity test, when there are corruptions induced.
   */
  @ContainerTestVersionInfo.ContainerTest
  public void testKeyValueContainerCheckCorruption(
      ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);
    long containerID = 102;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration sc = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks, true);
    KeyValueContainerData containerData = container.getContainerData();

    container.close();

    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(conf, container);

    File dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(containerData);
    containerData.setDbFile(dbFile);
    try (DBHandle ignored = BlockUtils.getDB(containerData, conf);
        BlockIterator<BlockData> kvIter =
                ignored.getStore().getBlockIterator(containerID)) {
      BlockData block = kvIter.nextBlock();
      assertFalse(block.getChunks().isEmpty());
      ContainerProtos.ChunkInfo c = block.getChunks().get(0);
      BlockID blockID = block.getBlockID();
      File chunkFile = getChunkLayout().getChunkFile(containerData, blockID, c.getChunkName());
      long length = chunkFile.length();
      assertThat(length).isGreaterThan(0);
      // forcefully truncate the file to induce failure.
      try (RandomAccessFile file = new RandomAccessFile(chunkFile, "rws")) {
        file.setLength(length / 2);
      }
      assertEquals(length / 2, chunkFile.length());
    }

    // metadata check should pass.
    boolean valid = kvCheck.fastCheck().isHealthy();
    assertTrue(valid);

    // checksum validation should fail.
    valid = kvCheck.fullCheck(new DataTransferThrottler(
            sc.getBandwidthPerVolume()), null).isHealthy();
    assertFalse(valid);
  }

  @ContainerTestVersionInfo.ContainerTest
  void testKeyValueContainerCheckDeletedContainer(ContainerTestVersionInfo versionInfo) throws Exception {
    initTestData(versionInfo);

    KeyValueContainer container = createContainerWithBlocks(3,
        3, 3, true);
    container.close();
    KeyValueContainerCheck kvCheck = new KeyValueContainerCheck(getConf(), container);
    // The full container should exist and pass a scan.
    ScanResult result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertTrue(result.isHealthy());
    assertFalse(result.isDeleted());

    // When a container is not marked for deletion and it has peices missing, the scan should fail.
    File metadataDir = new File(container.getContainerData().getChunksPath());
    FileUtils.deleteDirectory(metadataDir);
    assertFalse(metadataDir.exists());
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertFalse(result.isHealthy());
    assertFalse(result.isDeleted());

    // Once the container is marked for deletion, the scan should pass even if some of the internal pieces are missing.
    // Here the metadata directory has been deleted.
    container.markContainerForDelete();
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertTrue(result.isHealthy());
    assertTrue(result.isDeleted());

    // Now the data directory is deleted.
    File chunksDir = new File(container.getContainerData().getChunksPath());
    FileUtils.deleteDirectory(chunksDir);
    assertFalse(chunksDir.exists());
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertTrue(result.isHealthy());
    assertTrue(result.isDeleted());

    // Now the whole container directory is gone.
    File containerDir = new File(container.getContainerData().getContainerPath());
    FileUtils.deleteDirectory(containerDir);
    assertFalse(containerDir.exists());
    result = kvCheck.fullCheck(mock(DataTransferThrottler.class), mock(Canceler.class));
    assertTrue(result.isHealthy());
    assertTrue(result.isDeleted());
  }
}
