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

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component version manager for Ozone Manager.
 */
public class OMVersionManager extends ComponentVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(OMVersionManager.class);

  private final Map<ComponentVersion, OmUpgradeAction> upgradeActions;

  private final OzoneManager om;

  public OMVersionManager(OzoneManager om) throws IOException {
    this(om, new OMUpgradeActionProvider());
  }

  public OMVersionManager(OzoneManager om, ComponentUpgradeActionProvider<OmUpgradeAction> upgradeActionProvider)
      throws IOException {
    super(om.getOmStorage(), computeApparentVersion(om.getOmStorage().getApparentVersion()),
        OzoneManagerVersion.SOFTWARE_VERSION);
    this.om = om;

    ComponentVersion dbVersion = getApparentVersionInDB();
    ComponentVersion apparentVersion = getApparentVersion();

    if (!apparentVersion.equals(dbVersion)) {
      LOG.info("Version File has different layout version ({}) than OM DB ({}). That is expected if this " +
              "OM has never been finalized to a newer layout version.", apparentVersion, dbVersion);
    }

    upgradeActions = upgradeActionProvider.load();
  }

  public void checkDBSnapshotFinalization() throws IOException {
    ComponentVersion apparentVersionInDB = getApparentVersionInDB();
    if (apparentVersionInDB != null && !isAllowed(apparentVersionInDB)) {
      LOG.info("New OM snapshot received with higher layout version {}. " +
          "Attempting to finalize current OM to that version.", apparentVersionInDB);
      finalizeUpgrade();
      updateApparentVersionInDB();
    }
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
      action.execute(om);
    } catch (Exception e) {
      logAndThrow(e, "OM upgrade action for version " + componentVersion + " failed.",
          UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    }
  }

  private ComponentVersion getApparentVersionInDB() throws IOException {
    String apparentVersion = om.getMetadataManager().getMetaTable().get(LAYOUT_VERSION_KEY);
    return (apparentVersion == null) ? null : computeApparentVersion(Integer.parseInt(apparentVersion));
  }

  private void updateApparentVersionInDB() throws IOException {
    om.getMetadataManager().getMetaTable().put(LAYOUT_VERSION_KEY, String.valueOf(getApparentVersion().serialize()));
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
