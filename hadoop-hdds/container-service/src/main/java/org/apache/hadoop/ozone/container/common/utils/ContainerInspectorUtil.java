/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerInspector;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerMetadataInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to manage container inspectors. New inspectors can be added
 * here to have them loaded and process containers on startup.
 */
public final class ContainerInspectorUtil {

  private static final List<ContainerInspector> INSPECTORS = new ArrayList<>();
  static {
    // If new inspectors need to be added, they should be added to this list.
    INSPECTORS.add(new KeyValueContainerMetadataInspector());
  }

  private ContainerInspectorUtil() { }

  public static void load() {
    for (ContainerInspector inspector: INSPECTORS) {
      inspector.load();
    }
  }

  public static void unload() {
    for (ContainerInspector inspector: INSPECTORS) {
      inspector.unload();
    }
  }

  public static boolean isReadOnly(ContainerProtos.ContainerType type) {
    boolean readOnly = true;
    for (ContainerInspector inspector: INSPECTORS) {
      if (inspector.getContainerType() == type) {
        if (!inspector.isReadOnly()) {
          readOnly = false;
          break;
        }
      }
    }
    return readOnly;
  }

  public static void process(ContainerData data, DatanodeStore store) {
    for (ContainerInspector inspector: INSPECTORS) {
      if (inspector.getContainerType() == data.getContainerType()) {
        inspector.process(data, store);
      }
    }
  }
}
