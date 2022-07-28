package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RollingDatanodeFinalizer {
  private List<DatanodeDetails> unfinalizedNodes;
  private List<DatanodeDetails> finalizingNodes;
  private List<DatanodeDetails> finalizedNodes;

  private NodeManager nodeManager;
  private PipelineManager pipelineManager;

  public RollingDatanodeFinalizer(NodeManager nodeManager,
      PipelineManager pipelineManager) {
    this.nodeManager = nodeManager;
    this.pipelineManager = pipelineManager;

    unfinalizedNodes = new ArrayList<>();
    finalizingNodes = new ArrayList<>();
    finalizedNodes = new ArrayList<>();

    // TODO need to build these sets before background pipeline creation starts.
    for (DatanodeDetails dn: nodeManager.getAllNodes()) {
      if (nodeManager.getNodeStatus(dn).equals(NodeStatus.inServiceHealthyReadOnly())) {
        if (nodeManager.getPipelines(dn).isEmpty()) {
          // unfinalized node that is not part of any pipelines.
          // We can finalize it since it is not in any pipelines.
          finalizingNodes.add(dn);
        } else {
          // unfinalized node that is part of pipelines.
          // Has not yet been processed for finalization.
          unfinalizedNodes.add(dn);
        }
      } else {
        finalizedNodes.add(dn);
      }
    }
  }

  private void finalizeDatanodes() {
    // TODO notify bpc one shot run when a node is added to the finalized set.

    // TODO on layout version report, only instruct to finalize if in the
    //  finalizing set.

    // The maximum number of nodes we can have pending finalization
    final int MAX_FINALIZING_NODES = 3;

    while(!unfinalizedNodes.isEmpty()) {
      // If there are less than 3 healthy nodes finalizing, get more nodes.
      int healthyFinalizingNodes =
          finalizingNodes.stream().filter(dn -> nodeManager.getNodeStatus(dn).equals(NodeStatus))
      if (finalizingNodes.size() < 3) {
        addNodeAndPeersForFinalization(unfinalizedNodes.get(0));
      }
    }

    // periodically check on the progress of finalizing nodes.
    while ()
  }

  private void addNodeAndPeersForFinalization(DatanodeDetails dn) {
    // TODO filter by state to only care about non-closed pipelines.
    Set<PipelineID> dnPipelines = nodeManager.getPipelines(dn);
    if (dnPipelines.isEmpty()) {
      // The chosen node is not part of any pipelines, so we can tell it to
      // finalize.
      finalizingNodes.add(dn);
      unfinalizedNodes.remove(dn);
    } else {
      // The chosen node is in at least one pipeline.
      // Choose nodes from one of its pipelines and finalize them together,
      // after closing all their pipelines.
      Pipeline peerPipeline =
          pipelineManager.getPipeline(dnPipelines.stream().findAny());
      for (DatanodeDetails peerInPipeline: peerPipeline.getNodes()) {
        unfinalizedNodes.remove(peerInPipeline);
        // Close all the peers pipelines.
        for (PipelineID pipelineID: nodeManager.getPipelines(peerInPipeline)) {
          pipelineManager.closePipeline(pipelineManager.getPipeline(pipelineID), true);
        }
        // Now that all the pipelines using this node are closed, it can be
        // processed for finalization.
        finalizingNodes.add(peerInPipeline);
      }
    }
  }
}
