package org.apache.hadoop.hdds.scm.upgrade;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;

/**
 * Tests SCM finalization operations on mocked upgrade state.
 */
public class TestScmFinalization {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmFinalization.class);

  /**
   * Order of finalization checkpoints within the enum is used to determine
   * which ones have been passed. If ordering within the enum is changed
   * finalization will not behave correctly.
   */
  @Test
  public void testCheckpointOrder() {
    FinalizationCheckpoint[] checkpoints = FinalizationCheckpoint.values();
    Assert.assertEquals(4, checkpoints.length);
    Assert.assertEquals(checkpoints[0],
        FinalizationCheckpoint.FINALIZATION_REQUIRED);
    Assert.assertEquals(checkpoints[1],
        FinalizationCheckpoint.FINALIZATION_STARTED);
    Assert.assertEquals(checkpoints[2],
        FinalizationCheckpoint.MLV_EQUALS_SLV);
    Assert.assertEquals(checkpoints[3],
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  /**
   * Tests that the correct checkpoint is returned based on the value of
   * SCM's layout version and the presence of the finalizing key.
   */
  @Test
  public void testUpgradeStateToCheckpointMapping() throws Exception {
    HDDSLayoutVersionManager versionManager =
        new HDDSLayoutVersionManager(
            HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    // State manager keeps upgrade information in memory as well as writing
    // it to disk, so we can mock the classes that handle disk ops for this
    // test.
    FinalizationStateManager stateManager =
        new FinalizationStateManagerTestImpl.Builder()
            .setVersionManager(versionManager)
            .setFinalizationStore(Mockito.mock(Table.class))
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(Mockito.mock(DBTransactionBuffer.class))
            .build();
    // This is normally handled by the FinalizationManager, which we do not
    // have in this test.
    stateManager.addReplicatedFinalizationStep(lf ->
        versionManager.finalized((HDDSLayoutFeature) lf));

    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.FINALIZATION_REQUIRED);
    stateManager.addFinalizingMark();
    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.FINALIZATION_STARTED);

    for (HDDSLayoutFeature feature: HDDSLayoutFeature.values()) {
      // Cannot finalize initial version since we are already there.
      if (!feature.equals(HDDSLayoutFeature.INITIAL_VERSION)) {
        stateManager.finalizeLayoutFeature(feature.layoutVersion());
        if (versionManager.needsFinalization()) {
          assertCurrentCheckpoint(stateManager,
              FinalizationCheckpoint.FINALIZATION_STARTED);
        } else {
          assertCurrentCheckpoint(stateManager,
              FinalizationCheckpoint.MLV_EQUALS_SLV);
        }
      }
    }
    // Make sure we reached this checkpoint if we finished finalizing all
    // layout features.
    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.MLV_EQUALS_SLV);

    stateManager.removeFinalizingMark();
    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  private void assertCurrentCheckpoint(FinalizationStateManager stateManager,
      FinalizationCheckpoint expectedCheckpoint) {
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      LOG.info("Comparing expected checkpoint {} to {}", expectedCheckpoint,
          checkpoint);
      if (expectedCheckpoint.compareTo(checkpoint) >= 0) {
        // If the expected current checkpoint is >= this checkpoint,
        // then this checkpoint should be crossed according to the state
        // manager.
        Assert.assertTrue(stateManager.crossedCheckpoint(checkpoint));
      } else {
        // Else if the expected current checkpoint is < this
        // checkpoint, then this checkpoint should not be crossed according to
        // the state manager.
        Assert.assertFalse(stateManager.crossedCheckpoint(checkpoint));
      }
    }
  }

  /**
   * Tests resuming finalization after a failure or leader change, where the
   * disk state will indicate which finalization checkpoint (and therefore
   * set of steps) the SCM must resume from.
   */
  @Test
  public void testResumeFinalizationFromCheckpoint() throws Exception {
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      testResumeFinalizationFromCheckpoint(checkpoint);
    }
  }

  public void testResumeFinalizationFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) throws Exception {
    LOG.info("Testing finalization beginning at checkpoint {}",
        initialCheckpoint);

    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    // After finalization, SCM will wait for at least one pipeline to be
    // created. It does not care about the contents of the pipeline list, so
    // just return something with length >= 1.
    Mockito.when(pipelineManager.getPipelines(Mockito.any(),
        Mockito.any())).thenReturn(Arrays.asList(null, null, null));
    // Create the table and version manager to appear as if we left off from in
    // progress finalization.
    Table<String, String> finalizationStore =
        getMockTableFromCheckpoint(initialCheckpoint);
    HDDSLayoutVersionManager versionManager =
        getMockVersionManagerFromCheckpoint(initialCheckpoint);
    SCMHAManager haManager = Mockito.mock(SCMHAManager.class);
    DBTransactionBuffer buffer = Mockito.mock(DBTransactionBuffer.class);
    Mockito.when(haManager.getDBTransactionBuffer()).thenReturn(buffer);
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    SCMStorageConfig storage = Mockito.mock(SCMStorageConfig.class);

    FinalizationStateManager stateManager =
        new FinalizationStateManagerTestImpl.Builder()
            .setVersionManager(versionManager)
            .setFinalizationStore(finalizationStore)
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(buffer)
            .build();

    FinalizationManager manager = new FinalizationManagerTestImpl.Builder()
        .setFinalizationStateManager(stateManager)
        .setConfiguration(new OzoneConfiguration())
        .setLayoutVersionManager(versionManager)
        .setPipelineManager(pipelineManager)
        .setNodeManager(nodeManager)
        .setStorage(storage)
        .setHAManager(SCMHAManagerStub.getInstance(true))
        .setFinalizationStore(finalizationStore)
        .build();

    // Execute upgrade finalization, then check that events happened in the
    // correct order.
    manager.finalizeUpgrade(UUID.randomUUID().toString());

    InOrder inOrder = Mockito.inOrder(buffer, pipelineManager, nodeManager,
        storage);

    // Once the initial checkpoint's operations are crossed, this count will
    // be increased to 1 to indicate where finalization should have resumed
    // from.
    VerificationMode count = Mockito.never();
    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_REQUIRED) {
      count = Mockito.times(1);
    }

    // First, SCM should mark that it is beginning finalization.
    inOrder.verify(buffer, count).addToBuffer(
        ArgumentMatchers.eq(finalizationStore),
        ArgumentMatchers.matches(OzoneConsts.FINALIZING_KEY),
        ArgumentMatchers.matches(""));

    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_STARTED) {
      count = Mockito.times(1);
    }

    // Next, all pipeline creation should be stopped.
    inOrder.verify(pipelineManager, count).freezePipelineCreation();

    // Next, each layout feature should be finalized.
    for (HDDSLayoutFeature feature: HDDSLayoutFeature.values()) {
      // Cannot finalize initial version since we are already there.
      if (!feature.equals(HDDSLayoutFeature.INITIAL_VERSION)) {
        inOrder.verify(storage, count)
            .setLayoutVersion(feature.layoutVersion());
        inOrder.verify(storage, count).persistCurrentState();
        // After MLV == SLV, all datanodes should be moved to healthy readonly.
        if (feature.layoutVersion() ==
            HDDSLayoutVersionManager.maxLayoutVersion()) {
          inOrder.verify(nodeManager, count).forceNodesToHealthyReadOnly();
        }
        inOrder.verify(buffer, count).addToBuffer(
            ArgumentMatchers.eq(finalizationStore),
            ArgumentMatchers.matches(OzoneConsts.LAYOUT_VERSION_KEY),
            ArgumentMatchers.eq(String.valueOf(feature.layoutVersion())));
      }
    }
    // If this was not called in the loop, there was an error. To detect this
    // mistake, verify again here.
    Mockito.verify(nodeManager, count).forceNodesToHealthyReadOnly();

    if (initialCheckpoint == FinalizationCheckpoint.MLV_EQUALS_SLV) {
      count = Mockito.times(1);
    }

    // Last, the finalizing mark is removed to indicate finalization is
    // complete.
    inOrder.verify(buffer, count).removeFromBuffer(
        ArgumentMatchers.eq(finalizationStore),
        ArgumentMatchers.matches(OzoneConsts.FINALIZING_KEY));

    // If the initial checkpoint was FINALIZATION_COMPLETE, no mocks should
    // have been invoked.
  }

  /**
   * On startup, the finalization table will be read to determine the
   * checkpoint we are resuming from. After this, the results will be stored
   * in memory and flushed to the table asynchronously by the buffer, so the
   * mock table can continue to return the initial values since the in memory
   * values will be used after the initial table read on start.
   *
   * Layout version stored in the table is only used for ratis snapshot
   * finalization, which is not covered in this test.
   */
  private Table<String, String> getMockTableFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) throws Exception {
    Table<String, String> finalizationStore = Mockito.mock(Table.class);
    Mockito.when(finalizationStore
            .isExist(ArgumentMatchers.eq(OzoneConsts.FINALIZING_KEY)))
        .thenReturn(initialCheckpoint.needsFinalizingMark());
    return finalizationStore;
  }

  /**
   * On startup, components will read their version file to get their current
   * layout version and initialize the version manager with that. Simulate
   * that here.
   */
  private HDDSLayoutVersionManager getMockVersionManagerFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) throws Exception {
    int layoutVersion = HDDSLayoutVersionManager.maxLayoutVersion();
    if (initialCheckpoint.needsMlvBehindSlv()) {
      layoutVersion = HDDSLayoutFeature.INITIAL_VERSION.layoutVersion();
    }
    return new HDDSLayoutVersionManager(layoutVersion);
  }
}
