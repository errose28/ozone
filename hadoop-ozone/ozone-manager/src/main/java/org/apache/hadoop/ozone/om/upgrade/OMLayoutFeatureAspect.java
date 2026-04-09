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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;

import java.io.IOException;
import java.lang.reflect.Method;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 'Aspect' for OM Layout Feature API. All methods annotated with the
 * specific annotation will have pre-processing done here to check layout
 * version compatibility.
 */
@Aspect
public class OMLayoutFeatureAspect {

  public static final String GET_VERSION_MANAGER_METHOD_NAME =
      "getOmVersionManager";
  private static final Logger LOG = LoggerFactory
      .getLogger(OMLayoutFeatureAspect.class);

  @Before("@annotation(DisallowedUntilLayoutVersion) && execution(* *(..))")
  public void checkLayoutFeature(JoinPoint joinPoint) throws IOException {
    LayoutFeature layoutFeature = ((MethodSignature) joinPoint.getSignature())
        .getMethod().getAnnotation(DisallowedUntilLayoutVersion.class)
        .value();
    final Object[] args = joinPoint.getArgs();
    if (joinPoint.getTarget() instanceof OzoneManagerRequestHandler) {
      OzoneManager ozoneManager = ((OzoneManagerRequestHandler)
          joinPoint.getTarget()).getOzoneManager();
      checkIsAllowed(joinPoint.getSignature().toShortString(),
          ozoneManager.getVersionManager(), layoutFeature);
    } else if (joinPoint.getTarget() instanceof OMClientRequest &&
        joinPoint.toShortString().endsWith(".preExecute(..))")) {
      // Get OzoneManager instance from preExecute first argument
      OzoneManager ozoneManager = (OzoneManager) args[0];
      checkIsAllowed(joinPoint.getSignature().toShortString(),
          ozoneManager.getVersionManager(), layoutFeature);
    } else {
      try {
        Method method = joinPoint.getTarget().getClass()
            .getMethod(GET_VERSION_MANAGER_METHOD_NAME);
        OMVersionManager ovm = (OMVersionManager) method.invoke(joinPoint.getTarget());
        checkIsAllowed(joinPoint.getSignature().toShortString(), ovm, layoutFeature);
      } catch (Exception ex) {
        throw new IOException("Unable to load version manager to validate request: " + joinPoint.toShortString(), ex);
      }
    }
  }

  private void checkIsAllowed(String operationName,
                              OMVersionManager omVersionManager,
                              LayoutFeature layoutFeature) throws OMException {
    if (!omVersionManager.isAllowed(layoutFeature)) {
      throw new OMException(String.format("Operation %s cannot be invoked " +
              "before finalization. It belongs to version %s. Current apparent version is %s",
          operationName,
          layoutFeature,
          omVersionManager.getApparentVersion()),
          NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
  }

  @Pointcut("execution(* " +
      "org.apache.hadoop.ozone.om.request.OMClientRequest+.preExecute(..)) " +
      "&& @this(org.apache.hadoop.ozone.om.upgrade.BelongsToLayoutVersion)")
  public void omRequestPointCut() {
  }

  @Before("omRequestPointCut()")
  public void beforeRequestApplyTxn(final JoinPoint joinPoint)
      throws OMException {

    BelongsToLayoutVersion annotation = joinPoint.getTarget().getClass()
        .getAnnotation(BelongsToLayoutVersion.class);
    if (annotation == null) {
      return;
    }

    Object[] args = joinPoint.getArgs();
    OzoneManager om = (OzoneManager) args[0];

    LayoutFeature lf = annotation.value();
    checkIsAllowed(joinPoint.getTarget().getClass().getSimpleName(),
        om.getVersionManager(), lf);
  }

  /**
   * Note: Without this, it occasionally throws NoSuchMethodError when running
   * the test.
   */
  public static OMLayoutFeatureAspect aspectOf() {
    return new OMLayoutFeatureAspect();
  }

}
