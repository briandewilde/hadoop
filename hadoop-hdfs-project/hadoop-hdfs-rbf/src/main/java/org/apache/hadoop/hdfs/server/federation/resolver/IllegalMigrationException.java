package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;


/**
 * Thrown by MigratingMountTableResolver for an illegal migration.
 */
public class IllegalMigrationException extends IOException {
  public IllegalMigrationException(String message) {
    super(message);
  }

  public IllegalMigrationException(String message, Throwable cause) {
    super(message, cause);
  }
}
