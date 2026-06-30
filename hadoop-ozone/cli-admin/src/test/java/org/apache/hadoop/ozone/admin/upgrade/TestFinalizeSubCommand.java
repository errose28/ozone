/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.admin.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QueryUpgradeStatusResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for {@link FinalizeSubCommand}.
 */
public class TestFinalizeSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private FinalizeSubCommand cmd;
  private OzoneManagerProtocol omClient;
  private long originalPollInterval;
  private boolean verbose;

  @BeforeEach
  public void setup() throws IOException {
    omClient = mock(OzoneManagerProtocol.class);
    doNothing().when(omClient).close();
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.ZDU));

    verbose = false;
    cmd = new FinalizeSubCommand() {
      @Override
      protected OzoneManagerProtocol getClient() throws Exception {
        return omClient;
      }

      @Override
      protected boolean isVerbose() {
        return verbose;
      }
    };
    originalPollInterval = FinalizeSubCommand.pollIntervalMillis;
    // Keep tests fast — 1 ms per poll instead of the default 5 s.
    FinalizeSubCommand.pollIntervalMillis = 1;
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    FinalizeSubCommand.pollIntervalMillis = originalPollInterval;
  }

  @Test
  public void testCommandRunsAndPrintsOutput() throws Exception {
    new CommandLine(cmd).parseArgs();
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Cluster finalization has been started"));
    verify(omClient).finalizeUpgrade();
  }

  @Test
  public void testClientIsClosedAfterSuccessfulCall() throws Exception {
    new CommandLine(cmd).parseArgs();
    cmd.call();

    verify(omClient).close();
  }

  @Test
  public void testExceptionFromServerIsPropagated() throws Exception {
    doThrow(new IOException("OM unavailable")).when(omClient).finalizeUpgrade();

    new CommandLine(cmd).parseArgs();
    assertThrows(IOException.class, cmd::call);

    // Client must still be closed even when finalizeUpgrade() throws.
    verify(omClient).close();
  }

  @Test
  public void testNonZduServerPrintsErrorAndReturnsNonZero() throws Exception {
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.DEFAULT_VERSION));

    new CommandLine(cmd).parseArgs();
    assertEquals(1, cmd.call());

    String errOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errOutput.contains("OM does not support zero downtime upgrade"));
    verify(omClient, never()).finalizeUpgrade();
  }

  @Test
  public void testWithoutWaitFlagDoesNotPollStatus() throws Exception {
    new CommandLine(cmd).parseArgs();
    assertEquals(0, cmd.call());

    verify(omClient, never()).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagCompletesImmediatelyWhenAlreadyFinalized() throws Exception {
    when(omClient.queryUpgradeStatus()).thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Finalization complete."));
    verify(omClient).finalizeUpgrade();
    verify(omClient, times(1)).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagPollsUntilFinalized() throws Exception {
    when(omClient.queryUpgradeStatus())
        .thenReturn(inProgressStatus(0, 3))
        .thenReturn(inProgressStatus(2, 3))
        .thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Waiting for finalization"));
    assertTrue(output.contains("Finalization complete."));
    verify(omClient, times(3)).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagInterruptIsHandledCleanly() throws Exception {
    // Make the poll interval long enough that the interrupt lands during sleep.
    FinalizeSubCommand.pollIntervalMillis = 60_000;
    when(omClient.queryUpgradeStatus()).thenReturn(inProgressStatus(0, 3));

    new CommandLine(cmd).parseArgs("--wait");

    Thread runner = new Thread(() -> {
      try {
        cmd.call();
      } catch (Exception ignored) {
        // assertion in main thread
      }
    });
    runner.start();
    // Give the runner a moment to start sleeping.
    Thread.sleep(50);
    runner.interrupt();
    runner.join(5_000);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Waiting interrupted"));
    verify(omClient, never()).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagIsResumableAfterCancel() throws Exception {
    // First invocation: block forever in Thread.sleep so we can interrupt.
    FinalizeSubCommand.pollIntervalMillis = 60_000;
    // Second invocation is expected to see a finalized cluster on the first poll.
    when(omClient.queryUpgradeStatus()).thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");

    Thread runner = new Thread(() -> {
      try {
        cmd.call();
      } catch (Exception ignored) {
        // First call swallows interrupt cleanly; nothing to assert from the side thread.
      }
    });
    runner.start();
    Thread.sleep(50);
    runner.interrupt();
    runner.join(5_000);

    String firstOutput = outContent.toString(DEFAULT_ENCODING);
    assertTrue(firstOutput.contains("Cluster finalization has been started"));
    assertTrue(firstOutput.contains("Waiting interrupted"));
    // Side thread was interrupted during the initial sleep, so the first poll never happened.
    verify(omClient, never()).queryUpgradeStatus();

    // Second invocation — reset output capture and shrink the poll interval so it exits promptly.
    outContent.reset();
    FinalizeSubCommand.pollIntervalMillis = 1;

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String secondOutput = outContent.toString(DEFAULT_ENCODING);
    assertTrue(secondOutput.contains("Cluster finalization has been started"));
    assertTrue(secondOutput.contains("Finalization complete."));
    // finalizeUpgrade must have been issued on both runs (idempotent server-side).
    verify(omClient, times(2)).finalizeUpgrade();
    // queryUpgradeStatus only ran during the second invocation.
    verify(omClient, times(1)).queryUpgradeStatus();
    // The client is closed after each try-with-resources block.
    verify(omClient, times(2)).close();
  }

  @Test
  public void testWaitFlagWithVerbosePrintsFullStatus() throws Exception {
    verbose = true;
    when(omClient.queryUpgradeStatus())
        .thenReturn(finalizedStatus(2, 2));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    // Verbose output uses the StatusSubCommand.printVerbose layout.
    assertTrue(output.contains("OM Finalized?"));
    assertTrue(output.contains("SCM Finalized?"));
    assertTrue(output.contains("Finalization complete."));
  }

  private static QueryUpgradeStatusResponse inProgressStatus(int dnFinalized, int dnTotal) {
    return QueryUpgradeStatusResponse.newBuilder()
        .setOmFinalized(false)
        .setHddsStatus(HddsProtos.UpgradeStatus.newBuilder()
            .setScmFinalized(false)
            .setNumDatanodesFinalized(dnFinalized)
            .setNumDatanodesTotal(dnTotal)
            .build())
        .build();
  }

  private static QueryUpgradeStatusResponse finalizedStatus(int dnFinalized, int dnTotal) {
    return QueryUpgradeStatusResponse.newBuilder()
        .setOmFinalized(true)
        .setHddsStatus(HddsProtos.UpgradeStatus.newBuilder()
            .setScmFinalized(true)
            .setNumDatanodesFinalized(dnFinalized)
            .setNumDatanodesTotal(dnTotal)
            .build())
        .build();
  }

  private ServiceInfoEx serviceInfoWithVersion(OzoneManagerVersion version) {
    ServiceInfo serviceInfo = new ServiceInfo.Builder()
        .setNodeType(HddsProtos.NodeType.OM)
        .setHostname("localhost")
        .setOmVersion(version)
        .build();
    return new ServiceInfoEx(Collections.singletonList(serviceInfo), "", Collections.emptyList());
  }
}
