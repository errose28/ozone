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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component version manager for Ozone Manager.
 */
public class OMVersionManager extends ComponentVersionManager {

  public static final String OM_UPGRADE_CLASS_PACKAGE = "org.apache.hadoop.ozone.om.upgrade";

  private static final Logger LOG = LoggerFactory.getLogger(OMVersionManager.class);

  private Map<ComponentVersion, OmUpgradeAction> upgradeActions;

  private final OzoneManager upgradeActionArg;

  public OMVersionManager(OzoneManager om) throws IOException {
    super(om.getOmStorage(), computeApparentVersion(om.getOmStorage().getApparentVersion()),
        OzoneManagerVersion.SOFTWARE_VERSION);
    upgradeActionArg = om;
    registerUpgradeActions();
  }

  protected void runUpgradeAction(ComponentVersion componentVersion) throws UpgradeException {
    try {
      upgradeActions.get(componentVersion).execute(upgradeActionArg);
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

  /**
   * Scan classpath and register all actions to layout features.
   *
   * TODO We will need a new annotation to register upgrade actions to component versions. Annotations cannot use
   *  interfaces as fields so the existing {@link UpgradeActionOm} cannot be made generic across layout features and
   *  component versions.
   */
  private void registerUpgradeActions() {
    upgradeActions = new HashMap<>();

    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .forPackages(OM_UPGRADE_CLASS_PACKAGE)
        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
        .setExpandSuperTypes(false)
        .setParallel(true));
    Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(UpgradeActionOm.class);

    typesAnnotatedWith.forEach(actionClass -> {
      if (OmUpgradeAction.class.isAssignableFrom(actionClass)) {
        try {
          OmUpgradeAction action = (OmUpgradeAction) actionClass.newInstance();
          UpgradeActionOm annotation =
              actionClass.getAnnotation(UpgradeActionOm.class);
          OMLayoutFeature feature = annotation.feature();
          if (!isAllowed(feature)) {
            LOG.info("Registering Upgrade Action : {}", action.name());
            upgradeActions.put(feature, action);
          } else {
            LOG.debug("Skipping Upgrade Action {} since it has been finalized.", action.name());
          }
        } catch (Exception e) {
          LOG.error("Cannot instantiate Upgrade Action class {}",
              actionClass.getSimpleName(), e);
        }
      } else {
        LOG.warn("Found upgrade action class not of type " +
                "org.apache.hadoop.ozone.om.upgrade.OmUpgradeAction : {}",
            actionClass.getName());
      }
    });
  }
}
