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

package org.apache.hadoop.ozone.om.request.validator;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Aggregates {@link RequestAction} preprocessing and {@link ResponseAction} postprocessing, ordered like the legacy
 * annotation-based validation stack (client versioning first, then server finalization probes).
 *
 * <p>Thread-safety follows the supplied mutable {@link Builder} maps; treat each builder as single-shot.
 */
public final class RequestValidator {
  private final PreFinalizedConditions preFinalizedConditions;
  private final OldClientConditions oldClientConditions;
  private final GeneralConditions generalConditions;

  private RequestValidator(PreFinalizedConditions preFinalizedConditions, OldClientConditions oldClientConditions,
      GeneralConditions generalConditions) {
    this.preFinalizedConditions = preFinalizedConditions;
    this.oldClientConditions = oldClientConditions;
    this.generalConditions = generalConditions;
  }

  public OMRequest preProcess(OMRequest request) throws IOException {
    request = generalConditions.preProcess(request);
    request = oldClientConditions.preProcess(request);
    request = preFinalizedConditions.preProcess(request);
    return request;
  }

  public OMResponse postProcess(OMRequest request, OMResponse response) throws IOException {
    response = generalConditions.postProcess(request, response);
    response = oldClientConditions.postProcess(request, response);
    response = preFinalizedConditions.postProcess(request,  response);
    return response;
  }

  /**
   * Provides a fluent API which is used to define request pre and post processing based on client and server versions.
   */
  public static final class Builder {
    private final PreFinalizedConditions preFinalizedConditions;
    private final OldClientConditions oldClientConditions;
    private final GeneralConditions generalConditions;

    public Builder(OzoneManager om) {
      preFinalizedConditions = new PreFinalizedConditions(om);
      oldClientConditions = new OldClientConditions(om);
      generalConditions = new GeneralConditions(om);
    }

    /** Begins chaining server-gated actions (cluster finalization / layout versions). */
    public PreFinalizedConditionBuilder untilServerVersion(ComponentVersion minimumVersion) {
      return new PreFinalizedConditionBuilder(minimumVersion, preFinalizedConditions);
    }

    /** Begins chaining client-protocol-gated compat rules. */
    public OldClientConditionBuilder untilClientVersion(ClientVersion minimumVersion) {
      return new OldClientConditionBuilder(minimumVersion, oldClientConditions);
    }

    public SnapshotConditionBuilder ifSnapshotDisabled() {
      return new SnapshotConditionBuilder(generalConditions);
    }

    /** Materializes validators around the aggregated tables. */
    public RequestValidator build() {
      return new RequestValidator(preFinalizedConditions, oldClientConditions, generalConditions);
    }

    /**
     * Fluent API.
     * Created Once on each untilVersion call
     * Given the one total PreFinalizedServerConditions object to add to
     */
    public static final class PreFinalizedConditionBuilder {
      private final ComponentVersion minimumVersion;
      private final PreFinalizedConditions serverConditions;

      private PreFinalizedConditionBuilder(ComponentVersion minimumVersion,
          PreFinalizedConditions serverConditions) {
        this.minimumVersion = minimumVersion;
        this.serverConditions = serverConditions;
      }

      public PreFinalizedConditionBuilder block(Type cmd) {
        return preProcess(cmd, RequestAction::blockPreFinalized);
      }

      public PreFinalizedConditionBuilder preProcess(Type cmd, RequestAction action) {
        serverConditions.addPreProcessAction(cmd, minimumVersion, action);
        return this;
      }

      public PreFinalizedConditionBuilder postProcess(Type cmd, ResponseAction action) {
        serverConditions.addPostProcessAction(cmd, minimumVersion, action);
        return this;
      }
    }

    /**
     * Part of the fluent API which allows constructing conditions based on old client versions.
     */
    public static final class OldClientConditionBuilder {
      private final ClientVersion minimumVersion;
      private final OldClientConditions clientConditions;

      private OldClientConditionBuilder(ClientVersion minimumVersion, OldClientConditions clientConditions) {
        this.minimumVersion = minimumVersion;
        this.clientConditions = clientConditions;
      }

      public OldClientConditionBuilder preProcess(Type cmd, RequestAction action) {
        clientConditions.addPreProcessAction(cmd, minimumVersion, action);
        return this;
      }

      public OldClientConditionBuilder postProcess(Type cmd, ResponseAction action) {
        clientConditions.addPostProcessAction(cmd, minimumVersion, action);
        return this;
      }
    }

    /**
     * Part of the fluent API which allows constructing conditions.
     */
    public static final class SnapshotConditionBuilder {
      private final GeneralConditions generalConditions;

      private SnapshotConditionBuilder(GeneralConditions generalConditions) {
        this.generalConditions = generalConditions;
      }

      public SnapshotConditionBuilder block(Type cmd) {
        generalConditions.addPreProcessAction(cmd, SNAPSHOT_PRE_PROCESS);
        return this;
      }

      private static final RequestAction SNAPSHOT_PRE_PROCESS = context -> {
        if (!context.isSnapshotEnabled()) {
          throw new OMException(String.format(
              "Operation %s cannot be invoked because Ozone snapshot feature is disabled.",
              context.getRequest().getCmdType()),
              FEATURE_NOT_ENABLED);
        }
        return context.getRequest();
      };
    }
  }

  private static final class PreFinalizedConditions {
    private final Map<Type, SortedMap<ComponentVersion, RequestAction>> preProcessActions;
    private final Map<Type, SortedMap<ComponentVersion, ResponseAction>> postProcessActions;
    private final OMVersionManager versionManager;
    private final OzoneManager contextArg;

    PreFinalizedConditions(OzoneManager contextArg) {
      this.preProcessActions = new HashMap<>();
      this.postProcessActions = new HashMap<>();
      this.versionManager = contextArg.getVersionManager();
      this.contextArg = contextArg;
    }

    public void addPreProcessAction(Type cmd, ComponentVersion minServerVersion, RequestAction action) {
      SortedMap<ComponentVersion, RequestAction> versionToAction =
          preProcessActions.computeIfAbsent(cmd, unused -> new TreeMap<>(ComponentVersion.COMPARATOR));
      if (versionToAction.containsKey(minServerVersion)) {
        throw new IllegalArgumentException(
            "Duplicate OM pre-process action for cmd=" + cmd + " server version=" + minServerVersion);
      }

      if (!versionManager.isAllowed(minServerVersion)) {
        versionToAction.put(minServerVersion, action);
      }
    }

    public void addPostProcessAction(Type cmd, ComponentVersion minServerVersion, ResponseAction action) {
      SortedMap<ComponentVersion, ResponseAction> versionToAction =
          postProcessActions.computeIfAbsent(cmd, unused -> new TreeMap<>(ComponentVersion.COMPARATOR));
      if (versionToAction.containsKey(minServerVersion)) {
        throw new IllegalArgumentException(
            "Duplicate OM post-process action for cmd=" + cmd + " server version=" + minServerVersion);
      }

      if (!versionManager.isAllowed(minServerVersion)) {
        versionToAction.put(minServerVersion, action);
      }
    }

    public OMRequest preProcess(OMRequest request) throws IOException {
      SortedMap<ComponentVersion, RequestAction> versionsToActions = preProcessActions.get(request.getCmdType());
      if (skipProcessing(versionsToActions)) {
        return request;
      }

      // Else, this request has actions mapped to it for some unfinalized versions.
      OMRequest effectiveRequest = request;
      for (RequestAction action : getUnfinalizedActions(versionsToActions)) {
        effectiveRequest = action.process(new RequestValidationContext(contextArg, effectiveRequest));
      }
      return effectiveRequest;
    }

    public OMResponse postProcess(OMRequest request, OMResponse response) throws IOException {
      SortedMap<ComponentVersion, ResponseAction> versionsToActions = postProcessActions.get(request.getCmdType());
      if (skipProcessing(versionsToActions)) {
        return response;
      }

      // Else, this request has actions mapped to it for some unfinalized versions.
      OMResponse effectiveResponse = response;
      for (ResponseAction action : getUnfinalizedActions(versionsToActions)) {
        effectiveResponse = action.process(new ResponseValidationContext(contextArg, request, effectiveResponse));
      }
      return effectiveResponse;
    }

    private boolean skipProcessing(Map<ComponentVersion, ?> versionsForRequest) {
      return versionManager.needsFinalization() || versionsForRequest == null || versionsForRequest.isEmpty();
    }

    private <T> Iterable<T> getUnfinalizedActions(SortedMap<ComponentVersion, T> versionsToActions) {
      return versionsToActions.tailMap(versionManager.getFirstUnfinalizedVersion()).values();
    }
  }

  private static final class OldClientConditions {
    private final Map<Type, SortedMap<ClientVersion, RequestAction>> preProcessActions;
    private final Map<Type, SortedMap<ClientVersion, ResponseAction>> postProcessActions;
    private final OzoneManager contextArg;

    OldClientConditions(OzoneManager contextArg) {
      this.preProcessActions = new HashMap<>();
      this.postProcessActions = new HashMap<>();
      this.contextArg = contextArg;
    }

    public void addPreProcessAction(Type cmd, ClientVersion minClientVersion, RequestAction action) {
      SortedMap<ClientVersion, RequestAction> versionToAction =
          preProcessActions.computeIfAbsent(cmd, unused -> new TreeMap<>(ClientVersion.COMPARATOR));
      if (versionToAction.containsKey(minClientVersion)) {
        throw new IllegalArgumentException(
            "Duplicate OM pre-process action for cmd=" + cmd + " client version=" + minClientVersion);
      }

      versionToAction.put(minClientVersion, action);
    }

    public void addPostProcessAction(Type cmd, ClientVersion minClientVersion, ResponseAction action) {
      SortedMap<ClientVersion, ResponseAction> versionToAction =
          postProcessActions.computeIfAbsent(cmd, unused -> new TreeMap<>(ClientVersion.COMPARATOR));
      if (versionToAction.containsKey(minClientVersion)) {
        throw new IllegalArgumentException(
            "Duplicate OM post-process action for cmd=" + cmd + " client version=" + minClientVersion);
      }

      versionToAction.put(minClientVersion, action);
    }

    public OMRequest preProcess(OMRequest request) throws IOException {
      ClientVersion clientVersion = ClientVersion.deserialize(request.getVersion());
      SortedMap<ClientVersion, RequestAction> versionsToActions = preProcessActions.get(request.getCmdType());
      if (skipProcessing(clientVersion, versionsToActions)) {
        return request;
      }

      // Else, this request has actions mapped to it for some unfinalized versions.
      OMRequest effectiveRequest = request;
      for (RequestAction action : getActionsForOldClient(clientVersion, versionsToActions)) {
        effectiveRequest = action.process(new RequestValidationContext(contextArg, effectiveRequest));
      }
      return effectiveRequest;
    }

    public OMResponse postProcess(OMRequest request, OMResponse response) throws IOException {
      ClientVersion clientVersion = ClientVersion.deserialize(request.getVersion());
      SortedMap<ClientVersion, ResponseAction> versionsToActions = postProcessActions.get(request.getCmdType());
      if (skipProcessing(clientVersion, versionsToActions)) {
        return response;
      }

      // Else, this request has actions mapped to it for some unfinalized versions.
      OMResponse effectiveResponse = response;
      for (ResponseAction action : getActionsForOldClient(clientVersion, versionsToActions)) {
        effectiveResponse = action.process(new ResponseValidationContext(contextArg, request, effectiveResponse));
      }
      return effectiveResponse;
    }

    private <T> Iterable<T> getActionsForOldClient(ClientVersion version,
        SortedMap<ClientVersion, T> versionsToActions) {
      return versionsToActions.tailMap(version.nextVersion()).values();
    }

    private boolean skipProcessing(ClientVersion clientVersion, Map<ClientVersion, ?> versionsForRequest) {
      return clientVersion.isSupportedBy(ClientVersion.CURRENT) ||
          versionsForRequest == null || versionsForRequest.isEmpty();
    }
  }

  /**
   * Conditions that are always checked.
   */
  private static final class GeneralConditions {
    private final Map<Type, RequestAction> preProcessActions;
    private final Map<Type, ResponseAction> postProcessActions;
    private final OzoneManager contextArg;

    GeneralConditions(OzoneManager contextArg) {
      this.contextArg = contextArg;
      preProcessActions = new EnumMap<>(Type.class);
      postProcessActions = new EnumMap<>(Type.class);
    }

    public void addPreProcessAction(Type cmd, RequestAction action) {
      preProcessActions.put(cmd, action);
    }

    public void addPostProcessAction(Type cmd, ResponseAction action) {
      postProcessActions.put(cmd, action);
    }

    public OMRequest preProcess(OMRequest request) throws IOException {
      RequestAction action = preProcessActions.get(request.getCmdType());
      if (action != null) {
        return action.process(new RequestValidationContext(contextArg, request));
      }
      return request;
    }

    public OMResponse postProcess(OMRequest request, OMResponse response) throws IOException {
      ResponseAction action = postProcessActions.get(request.getCmdType());
      if (action != null) {
        return action.process(new ResponseValidationContext(contextArg, request, response));
      }
      return response;
    }
  }
}
