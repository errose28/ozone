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

package org.apache.hadoop.ozone.om.upgrade;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;

/**
 * Component version manager for Ozone Manager.
 */
public class OMVersionManager extends ComponentVersionManager {

  private final Map<ComponentVersion, OmUpgradeAction> upgradeActions;

  private final OzoneManager upgradeActionArg;

  public OMVersionManager(OzoneManager om) throws IOException {
    this(om, new OMUpgradeActionProvider());
  }

  public OMVersionManager(OzoneManager om, ComponentUpgradeActionProvider<OmUpgradeAction> upgradeActionProvider)
      throws IOException {
    super(om.getOmStorage(), computeApparentVersion(om.getOmStorage().getApparentVersion()),
        OzoneManagerVersion.SOFTWARE_VERSION);
    upgradeActionArg = om;
    upgradeActions = upgradeActionProvider.load();
  }

  @VisibleForTesting
  public Map<ComponentVersion, OmUpgradeAction> getUpgradeActionsForTesting() {
    return upgradeActions;
  }

  protected void runUpgradeAction(ComponentVersion componentVersion) throws UpgradeException {
    OmUpgradeAction action = upgradeActions.get(componentVersion);
    if (action == null) {
      return;
    }
    try {
      action.execute(upgradeActionArg);
    } catch (Exception e) {
      logAndThrow(e, "OM upgrade action for version " + componentVersion + " failed.",
          UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    }
  }

  /**
   * If the apparent version stored on the disk is >= 100, it indicates the component has been finalized for the
   * ZDU feature, and the apparent version corresponds to a version in {@link OzoneManagerVersion}.
   * If the apparent version stored on the disk is < 100, it indicates the component is not yet finalized for the
   * ZDU feature, and the apparent version corresponds to a version in {@link OMLayoutFeature}.
   */
  private static ComponentVersion computeApparentVersion(int serializedApparentVersion) {
    if (serializedApparentVersion < OzoneManagerVersion.ZDU.serialize()) {
      return OMLayoutFeature.deserialize(serializedApparentVersion);
    } else {
      return OzoneManagerVersion.deserialize(serializedApparentVersion);
    }
  }
}
