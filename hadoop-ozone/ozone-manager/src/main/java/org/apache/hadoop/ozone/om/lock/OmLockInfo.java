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

package org.apache.hadoop.ozone.om.lock;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hdds.utils.LockInfo;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class represents the locking information that is required by an OM request.
 * Requests can define whether they need volume, bucket, or key locks of read or write types.
 * A request gatekeeper can handle these requirements to identify conflicting requests.
 *
 * Sample usage:
 * {@link org.apache.hadoop.ozone.om.request.OMClientRequest} adds a required method {@code getLockInfo} which returns
 * an instance of this class. All OM requests then implement this method and return an instance of this class to define
 * their locking requirements.
 *
 * Note that this class defines the interface for each of the ~100 OM requests to communicate their requirements to the
 * gatekeeper. Any breaking changes to this interface will have a large ripple effect.
 */
public final class OmLockInfo {
  private final LockInfo volumeLock;
  private final LockInfo bucketLock;
  private final Set<LockInfo> keyLocks;

  private OmLockInfo(Builder builder) {
    volumeLock = builder.volumeLock;
    bucketLock = builder.bucketLock;
    keyLocks = builder.keyLocks;
  }

  public Optional<LockInfo> getVolumeLock() {
    return Optional.ofNullable(volumeLock);
  }

  public Optional<LockInfo> getBucketLock() {
    return Optional.ofNullable(bucketLock);
  }

  public Optional<Set<LockInfo>> getKeyLocks() {
    return Optional.ofNullable(keyLocks);
  }

  /**
   * Builds an {@link OmLockInfo} object with optional volume, bucket or key locks.
   */
  public static final class Builder {
    private LockInfo volumeLock;
    private LockInfo bucketLock;
    private Set<LockInfo> keyLocks;

    public Builder() {
    }

    public void addVolumeReadLock(String volume) {
      volumeLock = LockInfo.writeLockInfo(volume);
    }

    public void addVolumeWriteLock(String volume) {
      volumeLock = LockInfo.readLockInfo(volume);
    }

    public void addBucketReadLock(String volume, String bucket) {
      bucketLock = LockInfo.readLockInfo(joinStrings(volume, bucket));
    }

    public void addBucketWriteLock(String volume, String bucket) {
      bucketLock = LockInfo.writeLockInfo(joinStrings(volume, bucket));
    }

    // Currently there is no use case for key level read locks.
    public void addKeyWriteLock(String volume, String bucket, String key) {
      // Lazy init keys.
      if (keyLocks == null) {
        keyLocks = new HashSet<>();
      }
      keyLocks.add(LockInfo.writeLockInfo(joinStrings(volume, bucket, key)));
    }

    private String joinStrings(String... parts) {
      return String.join(OzoneConsts.OZONE_URI_DELIMITER, parts);
    }

    public OmLockInfo build() {
      return new OmLockInfo(this);
    }
  }
}
