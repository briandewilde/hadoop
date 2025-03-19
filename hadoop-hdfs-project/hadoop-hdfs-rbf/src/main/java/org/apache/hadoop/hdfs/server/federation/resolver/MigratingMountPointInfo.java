package org.apache.hadoop.hdfs.server.federation.resolver;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * An immutable class that holds information about a migrating mount point.
 */
public class MigratingMountPointInfo {
  /** Source namespace from which the path is being migrated **/
  private final String srcNs;

  /** Destination namespace to which the path is being migrated **/
  private final String dstNs;
  
  public MigratingMountPointInfo(String srcNs, String dstNs) {
    this.srcNs = srcNs;
    this.dstNs = dstNs;
  }
  
  public String getSrcNs() {
    return srcNs;
  }
  
  public String getDstNs() {
    return dstNs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MigratingMountPointInfo that = (MigratingMountPointInfo) o;

    return new EqualsBuilder().append(getSrcNs(), that.getSrcNs())
        .append(getDstNs(), that.getDstNs())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(getSrcNs())
        .append(getDstNs())
        .toHashCode();
  }
}
