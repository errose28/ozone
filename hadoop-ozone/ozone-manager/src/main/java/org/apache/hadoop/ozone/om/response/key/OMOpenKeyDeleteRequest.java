/**
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
package org.apache.hadoop.ozone.om.response.key;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

public class OMOpenKeyDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMOpenKeyDeleteRequest.class);

  public OMOpenKeyDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    // Wrapper for list of key names.
    OzoneManagerProtocolProtos.OpenKeyDeleteRequest openKeyDeleteRequest =
            getOmRequest().getOpenKeyDeleteRequest();

    List<DeletedKeys> expiredOpenKeys =
            openKeyDeleteRequest.getDeletedOpenKeys();

    // TODO: Audit logging to track operations.
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    // TODO: Add metrics count for number of deleted open keys.
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();

    // TODO: Audit logging (used again after lock)
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
            OmResponseUtil.getOMResponseBuilder(getOmRequest());

    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;
    try {
      for (DeletedKeys expiredOpenKeysPerBucket: expiredOpenKeys) {
        updateCache(ozoneManager, trxnLogIndex, expiredOpenKeysPerBucket);
      }

      omResponse.setDeleteOpenKeyResponse(
              OzoneManagerProtocolProtos.OpenKeyDeleteResponse.newBuilder());
      omClientResponse = new OMOpenKeyDeleteResponse(omResponse, omKeyInfo,
              ozoneManager.isRatisEnabled());

      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyDeleteResponse(
              createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
              omDoubleBufferHelper);
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_OPEN_KEY, auditMap,
            exception, userInfo));


    switch (result) {
    case SUCCESS:
      // TODO: Add Metrics
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
              bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.",
              volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMOpenKeyDeleteRequest: {}",
              openKeyDeleteRequest);
    }

    return omClientResponse;
  }

  private void updateCache(OzoneManager ozoneManager, long trxnLogIndex,
                           DeletedKeys expiredKeysInBucket) throws IOException {

    boolean acquiredLock = false;
    String volumeName = expiredKeysInBucket.getVolumeName();
    String bucketName = expiredKeysInBucket.getBucketName();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      acquiredLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName);

      for (String keyName: expiredKeysInBucket.getKeysList()) {
        String fullKeyName = omMetadataManager.getOzoneKey(volumeName,
                bucketName, keyName);

        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(fullKeyName);
        if (omKeyInfo != null) {
          // Set the UpdateID to current transactionLogIndex
          omKeyInfo.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

          // Update table cache.
          omMetadataManager.getKeyTable().addCacheEntry(
                  new CacheKey<>(fullKeyName),
                  new CacheValue<>(Optional.absent(), trxnLogIndex));

          // No need to add cache entries to delete table. As delete table will
          // be used by DeleteKeyService only, not used for any client response
          // validation, so we don't need to add to cache.
        }
      }
    } finally {
      if (acquiredLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
                bucketName);
      }
    }
  }
}
