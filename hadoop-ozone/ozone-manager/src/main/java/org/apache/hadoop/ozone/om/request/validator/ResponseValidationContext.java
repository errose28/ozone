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
import java.util.function.UnaryOperator;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/** Response phase validation context. */
public final class ResponseValidationContext extends ValidationContext {

  private final OMResponse response;

  public ResponseValidationContext(
      OzoneManager om,
      OMRequest request,
      OMResponse response) {
    super(om, request);
    this.response = response;
  }

  public OMResponse getResponse() {
    return response;
  }

  /**
   * When the resolved bucket is non-legacy, returns an error response built from the current response
   * with {@link OzoneManagerProtocolProtos.Status#NOT_SUPPORTED_OPERATION} and the standard bucket-layout
   * upgrade message, after {@code customizeError} adjusts the builder (typically to clear the RPC payload).
   * Otherwise returns the current response unchanged.
   */
  public OMResponse createNonLegacyBucketResponse(
      String volumeName,
      String bucketName,
      UnaryOperator<OMResponse.Builder> customizeError) throws IOException {
    if (isLegacyBucket(volumeName, bucketName)) {
      return response;
    }
    String message =
        "Client is attempting to access bucket /" + volumeName + "/" + bucketName
            + " which uses non-LEGACY bucket layout features. Please upgrade the client"
            + " to a compatible version before performing this operation.";
    OMResponse.Builder builder =
        response.toBuilder().setStatus(OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION).setMessage(message);
    return customizeError.apply(builder).build();
  }
}
