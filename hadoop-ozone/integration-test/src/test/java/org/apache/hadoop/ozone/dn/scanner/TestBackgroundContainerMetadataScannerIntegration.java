package org.apache.hadoop.ozone.dn.scanner;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerMetadataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for the background container metadata scanner. This
 * scanner does a quick check of container metadata to find obvious failures
 * faster than a full data scan.
 */
@RunWith(Parameterized.class)
public class TestBackgroundContainerMetadataScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  private final ContainerCorruptions corruption;

  @Parameterized.Parameters(name="{0}")
  public static Collection<Object[]> supportedCorruptionTypes() {
    return ContainerCorruptions.getAllParamsExcept(
        ContainerCorruptions.MISSING_BLOCK,
        ContainerCorruptions.CORRUPT_BLOCK);
  }

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    // Speed up SCM closing of open container when an unhealthy replica is
    // reported.
    ReplicationManager.ReplicationManagerConfiguration rmConf = ozoneConfig
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    rmConf.setInterval(Duration.ofSeconds(1));
    ozoneConfig.setFromObject(rmConf);

    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED, true);
    // Make sure the background data scanner does not detect failures
    // before the metadata scanner under test does.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_DATA_ENABLED,
        false);
    // Make the background metadata scanner run frequently to reduce test time.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    buildCluster(ozoneConfig);
  }

  public TestBackgroundContainerMetadataScannerIntegration(
      ContainerCorruptions corruption) {
    this.corruption = corruption;
  }

  /**
   * {@link BackgroundContainerMetadataScanner} should detect corrupted metadata
   * in open or closed containers without client interaction.
   */
  @Test
  public void testCorruptionDetected() throws Exception {
    // Write data to an open and closed container.
    long closedContainerID = writeDataThenCloseContainer();
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        getDnContainer(closedContainerID).getContainerState());
    long openContainerID = writeDataToOpenContainer();
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
        getDnContainer(openContainerID).getContainerState());

    // Corrupt both containers.
    corruption.applyTo(getDnContainer(closedContainerID));
    corruption.applyTo(getDnContainer(openContainerID));
    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(() ->
            getDnContainer(closedContainerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        500, 5000);
    GenericTestUtils.waitFor(() ->
            getDnContainer(openContainerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        500, 5000);

    // Wait for SCM to get reports of the unhealthy replicas.
    waitForScmToSeeUnhealthyReplica(closedContainerID);
    waitForScmToSeeUnhealthyReplica(openContainerID);
    // Once the unhealthy replica is reported, the open container's lifecycle
    // state in SCM should move to closed.
    waitForScmToCloseContainer(openContainerID);
  }
}
