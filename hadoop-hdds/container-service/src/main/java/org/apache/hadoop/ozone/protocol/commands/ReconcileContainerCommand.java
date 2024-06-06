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

package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconcileContainerCommandProto;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Asks datanodes to reconcile the specified container with other container replicas.
 */
public class ReconcileContainerCommand extends SCMCommand<ReconcileContainerCommandProto> {

  private final List<DatanodeDetails> peerDatanodes;

  public ReconcileContainerCommand(long containerID, List<DatanodeDetails> peerDatanodes) {
    // Container ID serves as command ID, since only one reconciliation should be in progress at a time.
    super(containerID);
    this.peerDatanodes = peerDatanodes;
  }


  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.reconcileContainerCommand;
  }

  @Override
  public ReconcileContainerCommandProto getProto() {
    ReconcileContainerCommandProto.Builder builder = ReconcileContainerCommandProto.newBuilder()
        .setContainerID(getId());
    for (DatanodeDetails dd : peerDatanodes) {
      builder.addPeers(dd.getProtoBufMessage());
    }
    return builder.build();
  }

  public List<DatanodeDetails> getPeerDatanodes() {
    return peerDatanodes;
  }

  public long getContainerID() {
    return getId();
  }

  public static ReconcileContainerCommand getFromProtobuf(ReconcileContainerCommandProto protoMessage) {
    Preconditions.checkNotNull(protoMessage);

    List<HddsProtos.DatanodeDetailsProto> peers = protoMessage.getPeersList();
    List<DatanodeDetails> peerNodes = !peers.isEmpty()
        ? peers.stream()
        .map(DatanodeDetails::getFromProtoBuf)
        .collect(Collectors.toList())
        : emptyList();

    return new ReconcileContainerCommand(protoMessage.getContainerID(), peerNodes);
  }

  @Override
  public String toString() {
    return getType() +
        ": containerId=" + getContainerID() +
        ", peerNodes=" + peerDatanodes;
  }
}
