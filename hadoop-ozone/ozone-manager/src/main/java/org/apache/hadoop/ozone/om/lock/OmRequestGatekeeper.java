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

import com.google.common.util.concurrent.Striped;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hdds.utils.LockInfo;

/**
 * This class allows acquiring and releasing locks defined in an {@link OmLockInfo} instance.
 * This can be used to allow non-conflicting OM requests to be executed in parallel.
 */
public class OmRequestGatekeeper {
  private final Striped<ReadWriteLock> volumeLocks;
  private final Striped<ReadWriteLock> bucketLocks;
  // Currently only key level write locks are required. This lets us use the bulkGet API from the Striped class.
  private final Striped<Lock> keyLocks;
  private final long timeoutMillis;

  // TODO Arbitrary values for now.
  private static final int NUM_VOLUME_STRIPES = 128;
  private static final int NUM_BUCKET_STRIPES = 1024;
  private static final int NUM_KEY_STRIPES = 4096;

  // If stats about the locks need to be added, they should be tracked here in the gatekeeper.

  public OmRequestGatekeeper(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
    volumeLocks = Striped.readWriteLock(NUM_VOLUME_STRIPES);
    bucketLocks = Striped.readWriteLock(NUM_BUCKET_STRIPES);
    keyLocks = Striped.lock(NUM_KEY_STRIPES);
  }

  /**
   * Acquires all the locks specified by the {@code lockInfo} parameter. Caller is responsible for releasing the locks
   * by closing the returned object. If an exception is thrown, no locks will be acquired when the method exits.
   * @param lockInfo Defines the locks to acquire.
   * @return A handle that can be closed to release the acquired locks.
   * @throws InterruptedException If the thread is interrupted while waiting to acquire some of the locks.
   * @throws TimeoutException If the timeout elapses while waiting to acquire any of the locks individually.
   */
  public AutoCloseable lock(OmLockInfo lockInfo) throws InterruptedException, TimeoutException {
    // Common case is 1 volume, 1 bucket, and at most 2 key locks.
    List<Lock> locks = new ArrayList<>(4);

    Optional<LockInfo> optionalVolumeLock = lockInfo.getVolumeLock();
    Optional<LockInfo> optionalBucketLock = lockInfo.getBucketLock();
    Optional<Set<LockInfo>> optionalKeyLocks = lockInfo.getKeyLocks();

    if (optionalVolumeLock.isPresent()) {
      LockInfo volumeLockInfo = optionalVolumeLock.get();
      if (volumeLockInfo.isWriteLock()) {
        locks.add(volumeLocks.get(volumeLockInfo.getKey()).writeLock());
      } else {
        locks.add(volumeLocks.get(volumeLockInfo.getKey()).readLock());
      }
    }

    if (optionalBucketLock.isPresent()) {
      LockInfo bucketLockInfo = optionalBucketLock.get();
      if (bucketLockInfo.isWriteLock()) {
        locks.add(bucketLocks.get(bucketLockInfo.getKey()).writeLock());
      } else {
        locks.add(bucketLocks.get(bucketLockInfo.getKey()).readLock());
      }
    }

    if (optionalKeyLocks.isPresent()) {
      for (Lock keyLock: keyLocks.bulkGet(optionalKeyLocks.get())) {
        locks.add(keyLock);
      }
    }

    acquireLocks(locks);
    return () -> releaseLocks(locks);
  }

  /*
  Optional: If we want more diagnostic info on the type of lock that failed to be acquired (volume, bucket, or key),
  We can make the parameter a list of objects that wrap the Lock with information about its type.

  Note that logging the specific volume, bucket or keys this lock was trying to acquire is not helpful and
  misleading because collisions within the stripe lock might we are blocked on a request for a completely
  different part of the namespace.
  Obtaining the thread ID that we were waiting on would be more useful, but there is no easy way to do that.
   */
  private void acquireLocks(List<Lock> locks) throws TimeoutException, InterruptedException {
    List<Lock> acquiredLocks = new ArrayList<>(locks.size());
    for (Lock lock: locks) {
      if (lock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) {
        try {
          acquiredLocks.add(lock);
        } catch (Throwable e) {
          // We acquired this lock but were unable to add it to our acquired locks list.
          lock.unlock();
          releaseLocks(acquiredLocks);
          throw e;
        }
      } else {
        releaseLocks(acquiredLocks);
        throw new TimeoutException("Failed to acquire lock after the given timeout.");
      }
    }
  }

  private void releaseLocks(List<Lock> locks) {
    ListIterator<Lock> reverseIterator = locks.listIterator();
    while (reverseIterator.hasPrevious()) {
      Lock lock = reverseIterator.previous();
      lock.unlock();
    }
  }
}
