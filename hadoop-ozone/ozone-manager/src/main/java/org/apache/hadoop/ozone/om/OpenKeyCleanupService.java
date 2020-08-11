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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the background service to delete hanging open keys.
 * Scan the metadata of om periodically to get
 * the keys with prefix "#open#" and ask scm to
 * delete metadata accordingly, if scm returns
 * success for keys, then clean up those keys.
 */
public class OpenKeyCleanupService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenKeyCleanupService.class);

  private final static int OPEN_KEY_DELETING_CORE_POOL_SIZE = 2;

  private final KeyManager keyManager;
  private final ScmBlockLocationProtocol scmClient;
  private final AtomicLong purgedOpenKeyCount;
  private final AtomicLong runCount;

  public OpenKeyCleanupService(ScmBlockLocationProtocol scmClient,
      KeyManager keyManager, long serviceInterval,
      long serviceTimeout) {
    super("OpenKeyCleanupService", serviceInterval, TimeUnit.SECONDS,
        OPEN_KEY_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.keyManager = keyManager;
    this.scmClient = scmClient;
    this.purgedOpenKeyCount = new AtomicLong(0L);
    this.runCount = new AtomicLong(0L);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new OpenKeyDeletingTask());
    return queue;
  }

  private class OpenKeyDeletingTask
      implements BackgroundTask<BackgroundTaskResult> {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      runCount.incrementAndGet();
      try {
        List<BlockGroup> keyBlocksList = keyManager.getExpiredOpenKeys();
        if (keyBlocksList.size() > 0) {
          int toDeleteSize = keyBlocksList.size();
          LOG.debug("Found {} to-delete open keys in OM", toDeleteSize);
          List<DeleteBlockGroupResult> results =
              scmClient.deleteKeyBlocks(keyBlocksList);
          int deletedSize = 0;
          for (DeleteBlockGroupResult result : results) {
            if (result.isSuccess()) {
              try {
                keyManager.deleteExpiredOpenKey(result.getObjectKey());
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Key {} deleted from OM DB", result.getObjectKey());
                }
                deletedSize += 1;
              } catch (IOException e) {
                LOG.warn("Failed to delete hanging-open key {}",
                    result.getObjectKey(), e);
              }
            } else {
              LOG.warn("Deleting open Key {} failed because some of the blocks"
                      + " were failed to delete, failed blocks: {}",
                  result.getObjectKey(),
                  StringUtils.join(",", result.getFailedBlocks()));
            }
          }
          LOG.info("Found {} expired open key entries, successfully " +
              "cleaned up {} entries", toDeleteSize, deletedSize);
          purgedOpenKeyCount.addAndGet(deletedSize);
          return results::size;
        } else {
          LOG.debug("No hanging open key found in OM");
        }
      } catch (IOException e) {
        LOG.error("Unable to get hanging open keys, retry in"
            + " next interval", e);
      }
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }

  public AtomicLong getPurgedOpenKeyCount() {
    return purgedOpenKeyCount;
  }

  public AtomicLong getRunCount() {
    return runCount;
  }
}
