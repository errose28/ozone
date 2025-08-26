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

package org.apache.hadoop.hdds.utils;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/**
 * This class provides specifications about a lock's requirements.
 */
public final class LockInfo implements Comparable<LockInfo> {
  private final String key;
  private final boolean isWriteLock;

  private LockInfo(String key, boolean isWriteLock) {
    this.key = key;
    this.isWriteLock = isWriteLock;
  }

  public static LockInfo writeLockInfo(String key) {
    return new LockInfo(key, true);
  }

  public static LockInfo readLockInfo(String key) {
    return new LockInfo(key, false);
  }

  public String getKey() {
    return key;
  }

  public boolean isWriteLock() {
    return isWriteLock;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LockInfo)) {
      return false;
    }
    LockInfo lockInfo = (LockInfo) o;
    return isWriteLock == lockInfo.isWriteLock && Objects.equals(key, lockInfo.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, isWriteLock);
  }

  @Override
  public int compareTo(@NotNull LockInfo other) {
    return Integer.compare(hashCode(), other.hashCode());
  }
}
