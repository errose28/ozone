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
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneManagerUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Adapter base for OM request/response validations (builder-driven validator).
 *
 * <p>Version gating stays in {@link RequestValidator}. Layout-related compatibility logic in migrated
 * validators only needs to distinguish LEGACY buckets from FILE_SYSTEM_OPTIMIZED / OBJECT_STORE (see {@linkplain
 * org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler legacy post-process validators}), which maps to
 * checking {@linkplain org.apache.hadoop.ozone.om.helpers.BucketLayout#LEGACY persisted layout LEGACY}.
 */
public abstract class ValidationContext {
  private final OzoneManager ozoneManager;
  private final OMRequest request;

  protected ValidationContext(OzoneManager ozoneManager, OMRequest request) {
    this.ozoneManager = ozoneManager;
    this.request = request;
  }

  public final OMRequest getRequest() {
    return request;
  }

  /**
   * Serialized client protocol revision from the protobuf {@linkplain OMRequest request} payload.
   */
  public final ClientVersion getClientProtocolVersion() {
    return ClientVersion.deserialize(request.getVersion());
  }

  /**
   * Persisted OM apparent version backing {@linkplain org.apache.hadoop.ozone.om.upgrade.OMVersionManager#isAllowed
   * capability checks}.
   */
  public final ComponentVersion getApparentVersion() {
    return ozoneManager.getVersionManager().getApparentVersion();
  }

  /**
   * Compiled-in OM {@linkplain org.apache.hadoop.ozone.om.upgrade.OMVersionManager#getSoftwareVersion() software
   * revision}.
   */
  public final ComponentVersion getSoftwareVersion() {
    return ozoneManager.getVersionManager().getSoftwareVersion();
  }

  /**
   * True when resolved bucket metadata reports {@linkplain org.apache.hadoop.ozone.om.helpers.BucketLayout#LEGACY
   * LEGACY} layout (including link buckets resolved to source).
   *
   * @see org.apache.hadoop.ozone.om.helpers.BucketLayout#isLegacy()
   */
  public final boolean isLegacyBucket(String volumeName, String bucketName)
      throws IOException {
    return OzoneManagerUtils.getBucketLayout(
            ozoneManager.getMetadataManager(), volumeName, bucketName)
        .isLegacy();
  }
}
