/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade Action to switch the default schema version used for new
 * containers from V1 to V2. Existing contianers will not be affected.
 */
public class DataNodeUpgradeActionSchemaV1ToV2
    implements HDDSUpgradeAction<DatanodeStateMachine> {

  public static final Logger LOG =
      LoggerFactory.getLogger(DataNodeUpgradeActionSchemaV1ToV2.class);
  @Override
  public void executeAction(DatanodeStateMachine arg) throws Exception {
    LOG.info("Switching default schema version for new containers from {} to " +
        "{}", OzoneConsts.SCHEMA_V1, OzoneConsts.SCHEMA_V2);


  }
}
