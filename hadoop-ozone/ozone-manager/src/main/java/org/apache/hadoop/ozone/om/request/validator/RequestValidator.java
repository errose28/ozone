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

import java.io.IOException;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
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
  private final OzoneManager om;

  /*
   * Type -> gated {@linkplain ComponentVersion} -> preprocessing action invoked while that gate rejects current state.
   */
  private final Map<Type, SortedMap<ComponentVersion, RequestAction>> preProcessServerActions;
  private final Map<Type, SortedMap<ComponentVersion, ResponseAction>> postProcessServerActions;
  /*
   * Type -> gated client {@linkplain ClientVersion} -> preprocessing action invoked when clients trail.
   */
  private final Map<Type, SortedMap<ClientVersion, RequestAction>> preProcessClientActions;
  private final Map<Type, SortedMap<ClientVersion, ResponseAction>> postProcessClientActions;

  private RequestValidator(
      OzoneManager ozoneManager,
      Map<Type, SortedMap<ComponentVersion, RequestAction>> preProcessServerActions,
      Map<Type, SortedMap<ComponentVersion, ResponseAction>> postProcessServerActions,
      Map<Type, SortedMap<ClientVersion, RequestAction>> preProcessClientActions,
      Map<Type, SortedMap<ClientVersion, ResponseAction>> postProcessClientActions) {

    this.om = ozoneManager;
    this.preProcessServerActions = preProcessServerActions;
    this.postProcessServerActions = postProcessServerActions;
    this.preProcessClientActions = preProcessClientActions;
    this.postProcessClientActions = postProcessClientActions;
  }

  public OMRequest preProcess(OMRequest request) throws IOException {
    OMRequest modifiedRequest = preProcessClient(request);
    return preProcessServer(modifiedRequest);
  }

  public OMResponse postProcess(OMRequest request, OMResponse response) throws IOException {
    OMResponse modifiedResponse = postProcessClient(request, response);
    return postProcessServer(request, modifiedResponse);
  }

  private OMRequest preProcessClient(OMRequest request) throws IOException {
    // We only need to handle older clients.
    if (ClientVersion.CURRENT.isSupportedBy(request.getVersion())) {
      return request;
    }

    SortedMap<ClientVersion, RequestAction> versionsToActions = preProcessClientActions.get(request.getCmdType());
    if (versionsToActions == null || versionsToActions.isEmpty()) {
      return request;
    }

    OMRequest effectiveRequest = request;
    for (Map.Entry<ClientVersion, RequestAction> entry : versionsToActions.entrySet()) {
      ClientVersion minimumVersion = entry.getKey();
      if (!minimumVersion.isSupportedBy(request.getVersion())) {
        RequestAction action = entry.getValue();
        effectiveRequest = action.process(new RequestValidationContext(om, effectiveRequest));
      }
    }
    return effectiveRequest;
  }

  private OMRequest preProcessServer(OMRequest request) throws IOException {
    OMVersionManager versionManager = om.getVersionManager();
    // We only need to check server version if the OM is not finalized.
    // Otherwise, checking server versions is up to the client to handle an old server.
    if (!versionManager.needsFinalization()) {
      return request;
    }

    SortedMap<ComponentVersion, RequestAction> versionsToActions = preProcessServerActions.get(request.getCmdType());
    if (versionsToActions == null || versionsToActions.isEmpty()) {
      return request;
    }

    OMRequest effectiveRequest = request;
    for (Map.Entry<ComponentVersion, RequestAction> entry : versionsToActions.entrySet()) {
      ComponentVersion minimumVersion = entry.getKey();
      if (!versionManager.isAllowed(minimumVersion)) {
        RequestAction action = entry.getValue();
        effectiveRequest = action.process(new RequestValidationContext(om, effectiveRequest));
      }
    }
    return effectiveRequest;
  }

  private OMResponse postProcessClient(OMRequest request, OMResponse response) throws IOException {
    if (ClientVersion.CURRENT.isSupportedBy(request.getVersion())) {
      return response;
    }

    SortedMap<ClientVersion, ResponseAction> versionsToActions = postProcessClientActions.get(request.getCmdType());
    if (versionsToActions == null || versionsToActions.isEmpty()) {
      return response;
    }

    // TODO search for request version in this map instead of iterating.
    OMResponse effectiveResponse = response;
    for (Map.Entry<ClientVersion, ResponseAction> entry : versionsToActions.entrySet()) {
      ClientVersion minimumVersion = entry.getKey();
      if (!minimumVersion.isSupportedBy(request.getVersion())) {
        ResponseAction action = entry.getValue();
        effectiveResponse = action.process(new ResponseValidationContext(om, request, effectiveResponse));
      }
    }
    return effectiveResponse;
  }

  private OMResponse postProcessServer(OMRequest request, OMResponse response) throws IOException {
    OMVersionManager versionManager = om.getVersionManager();
    if (!versionManager.needsFinalization()) {
      return response;
    }

    SortedMap<ComponentVersion, ResponseAction> versionsToActions = postProcessServerActions.get(request.getCmdType());
    if (versionsToActions == null || versionsToActions.isEmpty()) {
      return response;
    }

    OMResponse effectiveResponse = response;
    for (Map.Entry<ComponentVersion, ResponseAction> entry : versionsToActions.entrySet()) {
      ComponentVersion minimumVersion = entry.getKey();
      if (!versionManager.isAllowed(minimumVersion)) {
        ResponseAction action = entry.getValue();
        effectiveResponse = action.process(new ResponseValidationContext(om, request, effectiveResponse));
      }
    }
    return effectiveResponse;
  }

  /**
   * Provides a fluent API which is used to define request pre and post processing based on client and server versions.
   */
  public static final class Builder {
    private final OzoneManager om;
    private final EnumMap<Type, SortedMap<ComponentVersion, RequestAction>> preProcessServerActions =
        new EnumMap<>(Type.class);
    private final EnumMap<Type, SortedMap<ComponentVersion, ResponseAction>> postProcessServerActions =
        new EnumMap<>(Type.class);

    private final EnumMap<Type, SortedMap<ClientVersion, RequestAction>> preProcessClientActions =
        new EnumMap<>(Type.class);
    private final EnumMap<Type, SortedMap<ClientVersion, ResponseAction>> postProcessClientActions =
        new EnumMap<>(Type.class);

    public Builder(OzoneManager ozoneManager) {
      this.om = ozoneManager;
    }

    /** Begins chaining server-gated actions (cluster finalization / layout versions). */
    public ServerRequestValidation untilServerVersion(ComponentVersion minimumVersion) {
      return new ServerRequestValidation(
          minimumVersion, om.getVersionManager(), preProcessServerActions, postProcessServerActions);
    }

    /** Begins chaining client-protocol-gated compat rules. */
    public ClientRequestValidation untilClientVersion(ClientVersion minimumVersion) {
      return new ClientRequestValidation(minimumVersion, preProcessClientActions, postProcessClientActions);
    }

    /** Materializes validators around the aggregated tables. */
    public RequestValidator build() {
      return new RequestValidator(
          om,
          preProcessServerActions,
          postProcessServerActions,
          preProcessClientActions,
          postProcessClientActions);
    }

    private static final Comparator<ComponentVersion> VERSION_COMPARATOR = (a, b) -> {
      if (Objects.equals(a, b)) {
        return 0;
      } else if (a.isSupportedBy(b)) {
        return -1;
      } else {
        return 1;
      }
    };

    /**
     * Fluent registrations for validations gated on apparent OM / layout {@linkplain ComponentVersion versions}.
     */
    public static final class ServerRequestValidation {
      private final ComponentVersion minimumVersion;
      private final OMVersionManager versionManager;
      private final Map<Type, SortedMap<ComponentVersion, RequestAction>> preProcessMap;
      private final Map<Type, SortedMap<ComponentVersion, ResponseAction>> postProcessMap;

      private ServerRequestValidation(ComponentVersion minimumVersion,
                              OMVersionManager versionManager,
                              Map<Type, SortedMap<ComponentVersion, RequestAction>> preProcessMap,
                              Map<Type, SortedMap<ComponentVersion, ResponseAction>> postProcessMap) {
        this.minimumVersion = minimumVersion;
        this.versionManager = versionManager;
        this.preProcessMap = preProcessMap;
        this.postProcessMap = postProcessMap;
      }

      public ServerRequestValidation block(Type cmd) {
        return preProcess(cmd, RequestAction::blockPreFinalized);
      }

      public ServerRequestValidation preProcess(Type cmd, RequestAction action) {
        SortedMap<ComponentVersion, RequestAction> versionToAction =
            preProcessMap.computeIfAbsent(cmd, unused -> new TreeMap<>(VERSION_COMPARATOR));
        if (versionToAction.containsKey(minimumVersion)) {
          throw new IllegalArgumentException(
              "Duplicate OM pre-process action for cmd=" + cmd + " version=" + minimumVersion);
        }

        if (!versionManager.isAllowed(minimumVersion)) {
          versionToAction.put(minimumVersion, action);
        }
        return this;
      }

      public ServerRequestValidation postProcess(Type cmd, ResponseAction action) {
        SortedMap<ComponentVersion, ResponseAction> versionToAction =
            postProcessMap.computeIfAbsent(cmd, unused -> new TreeMap<>(VERSION_COMPARATOR));
        if (versionToAction.containsKey(minimumVersion)) {
          throw new IllegalArgumentException(
              "Duplicate OM post-process action for cmd=" + cmd + " version=" + minimumVersion);
        }

        if (!versionManager.isAllowed(minimumVersion)) {
          versionToAction.put(minimumVersion, action);
        }
        return this;
      }
    }

    /**
     * Fluent registrations for validations gated strictly on OM client protocol versioning.
     *
     * Unlike {@linkplain ServerRequestValidation server-gated validations}, rules are unconditional at
     * registration-time (clients evolve faster than disk snapshots of apparent versions).
     */
    public static final class ClientRequestValidation {

      private final ClientVersion minimumVersion;
      private final Map<Type, SortedMap<ClientVersion, RequestAction>> preProcessMap;
      private final Map<Type, SortedMap<ClientVersion, ResponseAction>> postProcessMap;

      private ClientRequestValidation(
          ClientVersion gate,
          Map<Type, SortedMap<ClientVersion, RequestAction>> preProcessMap,
          Map<Type, SortedMap<ClientVersion, ResponseAction>> postProcessMap) {

        this.minimumVersion = gate;
        this.preProcessMap = preProcessMap;
        this.postProcessMap = postProcessMap;
      }

      public ClientRequestValidation preprocess(Type cmd, RequestAction action) {
        SortedMap<ClientVersion, RequestAction> versionToAction =
            preProcessMap.computeIfAbsent(cmd, unused -> new TreeMap<>(VERSION_COMPARATOR));
        if (versionToAction.containsKey(minimumVersion)) {
          throw new IllegalArgumentException(
              "Duplicate OM client pre-process action for cmd=" + cmd + " version=" + minimumVersion);
        }
        versionToAction.put(minimumVersion, action);
        return this;
      }

      public ClientRequestValidation postprocess(Type cmd, ResponseAction action) {
        SortedMap<ClientVersion, ResponseAction> versionToAction =
            postProcessMap.computeIfAbsent(cmd, unused -> new TreeMap<>(VERSION_COMPARATOR));
        if (versionToAction.containsKey(minimumVersion)) {
          throw new IllegalArgumentException(
              "Duplicate OM client post-process action for cmd=" + cmd + " version=" + minimumVersion);
        }
        versionToAction.put(minimumVersion, action);
        return this;
      }
    }
  }
}
