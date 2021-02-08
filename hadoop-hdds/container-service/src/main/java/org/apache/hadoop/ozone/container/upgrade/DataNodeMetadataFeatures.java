package org.apache.hadoop.ozone.container.upgrade;

public final class DataNodeMetadataFeatures {
  private static String schemaVersion;

  public static void setSchemaVersion(String version) {
    schemaVersion = version;
  }

  public static String getSchemaVersion() {
    return schemaVersion;
  }
}
