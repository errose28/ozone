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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/** Request phase validation context ({@linkplain OMRequest protobuf request}). */
public final class RequestValidationContext extends ValidationContext {

  public RequestValidationContext(OzoneManager om, OMRequest request) {
    super(om, request);
  }

  public void checkNonLegacyBucket(String volumeName, String bucketName) throws IOException {
    if (!isLegacyBucket(volumeName, bucketName)) {
      throw new OMException("Client is attempting to modify bucket /" + volumeName + "/" + bucketName
          + " which uses non-LEGACY bucket layout features. Please upgrade the client"
          + " to a compatible version before performing this operation.",
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
  }
}
