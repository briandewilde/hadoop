package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMigratingMountTableResolver {
  private static final String path = "/mp0";
  private static MigratingMountTableResolver resolver;

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    conf.setStrings(DFSConfigKeys.DFS_NAMESERVICES, "ns0", "ns1");
    resolver = new MigratingMountTableResolver(conf, null);

    setupMountTableEntry();
  }

  /**
   * Set up a mount table entrry in a default (non-migrating) state.
   * @param nsIds Optional namespace IDs to add to the mount table entry;
   *              if none are provided, only ns0 is added.
   * @return The mount table entry that was added.
   * @throws IOException If there is an error adding the mount table entry.
   */
  private MountTable setupMountTableEntry(String... nsIds) throws IOException {
    Map<String, String> destMap = new HashMap<>();
    if (nsIds.length == 0) {
      destMap.put("ns0", path);
    } else {
      for (String nsId : nsIds) {
        destMap.put(nsId, path);
      }
    }
    MountTable entry = MountTable.newInstance(path, destMap);
    // Overwrite any existing entry
    resolver.addEntry(entry);
    return entry;
  }

  /**
   * Set up a mount table entry in a migrating state.
   * @return The mount table entry that was added.
   * @throws IOException If there is an error adding the mount table entry.
   */
  private MountTable setupMigratingMountTableEntry() throws IOException {
    MountTable entry = setupMountTableEntry();
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    entry.addDestination("ns1", path);
    // Overwrite any existing entry
    resolver.addEntry(entry);
    return entry;
  }

  @Test
  public void testReconcileNormalEntryForOneNs() throws IOException {
    MountTable entry = setupMountTableEntry();
    MigratingMountTableResolver.reconcileEntryWithMigration(entry, null);
  }

  @Test
  public void testReconcileMigrationEntryForTwoNs() throws IOException {
    MountTable entry = setupMountTableEntry("ns0", "ns1");
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MountTable migrationEntry =
        MigratingMountTableResolver.reconcileEntryWithMigration(entry, null);
    Assert.assertTrue(migrationEntry.getDestinations().stream()
        .map(RemoteLocation::getNameserviceId)
        .collect(Collectors.toSet()).containsAll(Arrays.asList("ns0", "ns1")));
  }
  
  @Test
  public void testReconcileMigrationEntryForSrcNsAddsDst() throws IOException {
    MountTable entry = setupMountTableEntry();
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MountTable migrationEntry =
        MigratingMountTableResolver.reconcileEntryWithMigration(entry, null);
    Assert.assertTrue(migrationEntry.getDestinations().stream()
        .map(RemoteLocation::getNameserviceId)
        .collect(Collectors.toSet()).containsAll(Arrays.asList("ns0", "ns1")));
  }

  @Test
  public void testReconcileMigrationEntryThrowsForDstNs() throws IOException {
    MountTable entry = setupMountTableEntry("ns1");
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    Assert.assertThrows(IOException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry,
            null));
  }
  
  @Test
  public void testReconcileMigrationEntryThrowsForSameNs() throws IOException {
    MountTable entry = setupMountTableEntry();
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns0"));
    Assert.assertThrows(IllegalMigrationException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry,
            null));
  }

  @Test
  public void testReconcileMigrationEntryThrowsForInvalidNs()
      throws IOException {
    MountTable entry = setupMountTableEntry();
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("nsZ", "ns0"));
    Assert.assertThrows(IllegalMigrationException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry,
            null));
  }

  @Test
  public void testReconcileMigrationEntryThrowsForInvalidLocation()
      throws IOException {
    MountTable entry = setupMountTableEntry("nsZ");
    entry.setMigratingMountPointInfo(new MigratingMountPointInfo("ns0", "ns1"));
    Assert.assertThrows(IllegalMigrationException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry,
            null));
  }

  @Test
  public void testReconcileMigrationEntryForRollback() throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", path);
    destMap.put("ns1", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    entry2.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns1", "ns0"));
    MountTable endEntry =
        MigratingMountTableResolver.reconcileEntryWithMigration(entry2, entry1);

    Assert.assertTrue(endEntry.getDestinations().stream()
        .map(RemoteLocation::getNameserviceId)
        .collect(Collectors.toSet()).containsAll(Arrays.asList("ns0", "ns1")));
    Assert.assertNotNull(endEntry.getMigratingMountPointInfo());
    Assert.assertEquals("ns0",
        endEntry.getMigratingMountPointInfo().getDstNs());
    Assert.assertEquals("ns1",
        endEntry.getMigratingMountPointInfo().getSrcNs());
  }

  @Test
  public void testReconcileMigrationEntryThrowsForInvalidRollback()
      throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", path);
    destMap.put("ns1", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    entry2.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns1", "ns1"));
    Assert.assertThrows(IllegalMigrationException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry2,
            entry1));
  }

  @Test
  public void testReconcileMigrationEntryThrowsForInvalidRollbackLocation()
      throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", path);
    destMap.put("nsZ", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    entry2.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns1", "ns1"));
    Assert.assertThrows(IllegalMigrationException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry1,
            entry2));
  }
  
  @Test
  public void testReconcileMigrationEntryCompletesKeepingSrc()
      throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    MountTable endEntry =
        MigratingMountTableResolver.reconcileEntryWithMigration(entry2, entry1);
    
    Assert.assertNull(endEntry.getMigratingMountPointInfo());
    Assert.assertEquals(1, endEntry.getDestinations().size());
    Assert.assertEquals("ns0",
        endEntry.getDestinations().iterator().next().getNameserviceId());
  }

  @Test
  public void testReconcileMigrationEntryCompletesKeepingDst()
      throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns1", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    MountTable endEntry =
        MigratingMountTableResolver.reconcileEntryWithMigration(entry2, entry1);

    Assert.assertNull(endEntry.getMigratingMountPointInfo());
    Assert.assertEquals(1, endEntry.getDestinations().size());
    Assert.assertEquals("ns1",
        endEntry.getDestinations().iterator().next().getNameserviceId());
  }
  
  @Test
  public void testReconcileMigrationEntryThrowsForRemovingSrc()
      throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns1", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    entry2.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    Assert.assertThrows(IllegalMigrationException.class,
        () -> MigratingMountTableResolver.reconcileEntryWithMigration(entry2,
            entry1));
  }
  
  @Test
  public void testReconcileMigrationEntryAddsDstForRemovingDst()
      throws IOException {
    MountTable entry1 = setupMountTableEntry("ns0");
    entry1.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MigratingMountTableResolver.reconcileEntryWithMigration(entry1, null);

    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", path);
    MountTable entry2 = MountTable.newInstance(path, destMap);
    entry2.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    MountTable migrationEntry =
        MigratingMountTableResolver.reconcileEntryWithMigration(entry2, entry1);
    Assert.assertTrue(migrationEntry.getDestinations().stream()
        .map(RemoteLocation::getNameserviceId)
        .collect(Collectors.toSet()).containsAll(Arrays.asList("ns0", "ns1")));
  }
}
