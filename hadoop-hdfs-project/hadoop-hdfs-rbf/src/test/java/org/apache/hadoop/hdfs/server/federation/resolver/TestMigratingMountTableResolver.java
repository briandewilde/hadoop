package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteResult;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;
import org.apache.hadoop.hdfs.server.federation.resolver.MigratingMountTableResolver.MigrationBehavior;
import org.mockito.ArgumentCaptor;
import org.mockito.exceptions.base.MockitoException;


public class TestMigratingMountTableResolver {
  private static final String path = "/mp0";
  private static MigratingMountTableResolver resolver;
  private static RouterRpcServer rpcServerMock;
  private static RouterRpcClient rpcClientMock;

  private final RemoteLocation locationSrc =
      new RemoteLocation("ns0", path, path);
  private final RemoteLocation locationDst =
      new RemoteLocation("ns1", path, path);
  private final HdfsFileStatus newerFileInfo =
      mock(HdfsLocatedFileStatus.class);
  private final HdfsFileStatus olderFileInfo =
      mock(HdfsLocatedFileStatus.class);

  private static final HdfsFileStatus presentFileInfo =
      mock(HdfsFileStatus.class);
  private static final HdfsFileStatus presentDirInfo =
      mock(HdfsFileStatus.class);

  public TestMigratingMountTableResolver() {
    when(newerFileInfo.getModificationTime()).thenReturn(2L);
    when(olderFileInfo.getModificationTime()).thenReturn(1L);
    when(presentFileInfo.isDirectory()).thenReturn(false);
    when(presentDirInfo.isDirectory()).thenReturn(true);
  }

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    conf.setStrings(DFSConfigKeys.DFS_NAMESERVICES, "ns0", "ns1");
    resolver = new MigratingMountTableResolver(conf, null);
    rpcServerMock = mock(RouterRpcServer.class);
    rpcClientMock = mock(RouterRpcClient.class);
    when(rpcServerMock.getRPCClient()).thenReturn(rpcClientMock);
    resolver.setRpcServer(rpcServerMock);

    setupMountTableEntry();
  }
  
  @After
  public void resetMocks() throws IOException {
    resolver.setMigrationBehavior(MigrationBehavior.UNDEFINED, path);
    // Because this test is not associated with an RPC call all invocations
    // will share the same (invalid) RPC call id. To better simulate caching,
    // reset the context between tests.
    resolver.resetContext();
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

  /**
   * Test the resolver without a migrating mount point. The resolver should
   * include both source and dest per the default MountTableResolver behavior,
   * ignoring the migration behavior.
   */
  @Test
  public void testResolverWithoutMigrationIncludesBoth()
      throws IOException {
    setupMountTableEntry("ns0", "ns1");
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    new TestHelper()
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, olderFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationSrc, locationDst)
        .assertNotInvoked(locationSrc, locationDst);
  }

  @Test
  public void testLatestBehaviorIncludesLatestSrc() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    new TestHelper()
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, olderFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationSrc)
        .assertExcludes(locationDst)
        .assertInvoked(locationSrc, locationDst);
  }

  @Test
  public void testLatestBehaviorIncludesLatestDst() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    new TestHelper()
        .addResult(locationSrc, olderFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, newerFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);
  }
  
  @Test
  public void testLatestBehaviorDefaultsToDst() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    new TestHelper()
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, newerFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);
  }
  
  @Test
  public void testLatestBehaviorAlwaysUsesDstDir() throws IOException {
    when(newerFileInfo.isDirectory()).thenReturn(true);
    when(olderFileInfo.isDirectory()).thenReturn(true);
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    new TestHelper()
        // For this test, use the newer modification time for the source
        // to ensure mod time is not considered for dirs
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, olderFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);
  }

  @Test
  public void testDstOnlyBehaviorExcludesSrc() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.DST_ONLY, path);
    new TestHelper()
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, newerFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);
  }
  
  @Test
  public void testDstOnlyBehaviorThrowsForOutOfDateDst() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.DST_ONLY, path);
    TestHelper helper = new TestHelper()
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, olderFileInfo, HdfsFileStatus.class);
    Assert.assertThrows(IOException.class, helper::evaluate);
    helper.assertInvoked(locationSrc, locationDst);
  }
  
  @Test
  public void testLeasedBehaviorIncludesLeasedSrc() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);
    LocatedBlocks blocksLeased = mock(LocatedBlocks.class);
    LocatedBlocks blocksOther = mock(LocatedBlocks.class);
    when(blocksLeased.isUnderConstruction()).thenReturn(true);
    when(blocksOther.isUnderConstruction()).thenReturn(false);

    new TestHelper()
        .addResult(locationSrc, blocksLeased, LocatedBlocks.class)
        .addResult(locationDst, blocksOther, LocatedBlocks.class)
        .evaluate()
        .assertIncludes(locationSrc)
        .assertExcludes(locationDst)
        .assertInvoked(locationSrc, locationDst);
  }

  @Test
  public void testLeasedBehaviorIncludesLeasedDst() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);
    LocatedBlocks blocksLeased = mock(LocatedBlocks.class);
    LocatedBlocks blocksOther = mock(LocatedBlocks.class);
    when(blocksLeased.isUnderConstruction()).thenReturn(true);
    when(blocksOther.isUnderConstruction()).thenReturn(false);

    new TestHelper()
        .addResult(locationSrc, blocksOther, LocatedBlocks.class)
        .addResult(locationDst, blocksLeased, LocatedBlocks.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);
  }

  @Test
  public void testLeasedBehaviorDefaultsToDst() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);
    LocatedBlocks blocksLeased = mock(LocatedBlocks.class);
    LocatedBlocks blocksOther = mock(LocatedBlocks.class);
    when(blocksLeased.isUnderConstruction()).thenReturn(false);
    when(blocksOther.isUnderConstruction()).thenReturn(false);

    new TestHelper()
        .addResult(locationSrc, blocksOther, LocatedBlocks.class)
        .addResult(locationDst, blocksLeased, LocatedBlocks.class)
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, newerFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);

    verify(rpcClientMock, times(1)).invokeConcurrent(anyList(),
        any(RemoteMethod.class), anyBoolean(), anyLong(),
        eq(LocatedBlocks.class));
    verify(rpcClientMock, times(1)).invokeConcurrent(anyList(),
        any(RemoteMethod.class), anyBoolean(), anyLong(),
        eq(HdfsFileStatus.class));
  }

  @Test
  public void testLeasedBehaviorHandlesMissingSrcBlocks()
      throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);
    LocatedBlocks blocksExist = mock(LocatedBlocks.class);
    when(blocksExist.isUnderConstruction()).thenReturn(false);

    // If source blocks are missing, the resolver should treat them as
    // non-leased. The destination is also non-leased, so this should default to
    // the destination.
    new TestHelper()
        .addResult(locationSrc, null, LocatedBlocks.class)
        .addResult(locationDst, blocksExist, LocatedBlocks.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc);
  }
  
  @Test
  public void testLeasedBehaviorHandlesMissingDstBlocks()
      throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);
    LocatedBlocks blocksExist = mock(LocatedBlocks.class);
    when(blocksExist.isUnderConstruction()).thenReturn(false);

    // If destination blocks are missing, the resolver should treat them as
    // non-leased. The source is also non-leased, so this should default to
    // the destination.
    new TestHelper()
        .addResult(locationSrc, blocksExist, LocatedBlocks.class)
        .addResult(locationDst, null, LocatedBlocks.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc);
  }

  @Test
  public void testLeasedBehaviorHandlesMissingSrcAndDstBlocks()
      throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);

    // If source and destination blocks are missing, the resolver should treat
    // them both as non-leased and default to the destination.
    new TestHelper()
        .addResult(locationSrc, null, LocatedBlocks.class)
        .addResult(locationDst, null, LocatedBlocks.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc);
  }
  
  @Test
  public void testUnionBehaviorIncludesBoth() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.UNION, path);
    new TestHelper()
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, olderFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationSrc, locationDst)
        .assertNotInvoked(locationSrc, locationDst);
  }
  
  @Test
  public void testUndefinedBehaviorThrows() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.UNDEFINED, path);
    IOException e = Assert.assertThrows(IOException.class,
            () -> new TestHelper().evaluate());
    Assert.assertTrue(
        e.getMessage().contains("Operation has no defined migration behavior"));
  }

  @Test
  public void testDefaultBehaviorIsUndefinedAndThrows() throws IOException {
    setupMigratingMountTableEntry();
    IOException e = Assert.assertThrows(IOException.class,
        () -> new TestHelper().evaluate());
    Assert.assertTrue(
        e.getMessage().contains("Operation has no defined migration behavior"));
  }
  
  @Test
  public void testDefaultBehaviorResetsToUndefinedAndThrows()
      throws IOException {
    setupMigratingMountTableEntry();
    // Set the RPC call ID to 1 to simulate an RPC call
    RPC.Server.getCurCall()
        .set(new Server.Call(1, 1, null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            "Test".getBytes()));
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    new TestHelper().evaluate();
    
    // Set the RPC call ID to 2 to simulate another RPC call on the same thread
    RPC.Server.getCurCall()
        .set(new Server.Call(2, 1, null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            "Test".getBytes()));
    IOException e = Assert.assertThrows(IOException.class,
        () -> new TestHelper().evaluate());
    Assert.assertTrue(
        e.getMessage().contains("Operation has no defined migration behavior"));
  }

  @Test
  public void testOtherPathFetchesRemoteLocationFromResolver()
      throws IOException {
    setupMigratingMountTableEntry();
    // Cache migration behavior and location for path
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    PathLocation pathLocation = resolver.getDestinationForPath(path + "/foo");
    // Cached location is for path, so assert pathLocation is path + "/foo"
    Assert.assertEquals(path + "/foo", pathLocation.getSourcePath());
  }
  
  @Test
  public void testComparingFilesAndDirectoriesFails() throws IOException {
    setupMigratingMountTableEntry();
    HdfsFileStatus fileInfo = mock(HdfsFileStatus.class);
    HdfsFileStatus dirInfo = mock(HdfsFileStatus.class);
    when(fileInfo.isDirectory()).thenReturn(false);
    when(dirInfo.isDirectory()).thenReturn(true);
    resolver.setMigrationBehavior(MigrationBehavior.LATEST, path);
    TestHelper helper = new TestHelper()
        .addResult(locationSrc, fileInfo, HdfsFileStatus.class)
        .addResult(locationDst, dirInfo, HdfsFileStatus.class);
    Assert.assertThrows(IOException.class, helper::evaluate);
  }

  /**
   * This is a special test to ensure that if a migrating mount point changes
   * during an operation, in this case a LEASED operation (which involves
   * multiple calls to NNs), the resolver will use the saved context to
   * determine the behavior.
   * @throws IOException If there is an error.
   */
  @Test
  public void testMigrationUsesSavedContextWithinOp() throws IOException {
    setupMigratingMountTableEntry();
    resolver.setMigrationBehavior(MigrationBehavior.LEASED, path);
    LocatedBlocks blocksLeased = mock(LocatedBlocks.class);
    LocatedBlocks blocksOther = mock(LocatedBlocks.class);
    when(blocksLeased.isUnderConstruction()).thenReturn(false);
    when(blocksOther.isUnderConstruction()).thenReturn(false);

    TestHelper helper = new TestHelper() {
      @Override
      void injectMocks() throws IOException {
        // Mock the RPC client to return the results
        for (Class<?> clazz : resultsMap.keySet()) {
          List<RemoteResult<RemoteLocation, ?>> results =
              new ArrayList<>(resultsMap.get(clazz));
          doAnswer(i -> {
            if (LocatedBlocks.class.isAssignableFrom(clazz)) {
              // Change so that the mount table is no longer migrating
              setupMountTableEntry();
            }
            return results;
          }).when(rpcClientMock)
              .invokeConcurrent(anyList(), any(RemoteMethod.class),
                  anyBoolean(), anyLong(), eq(clazz));
        }
      }
    };
    helper
        .addResult(locationSrc, blocksOther, LocatedBlocks.class)
        .addResult(locationDst, blocksLeased, LocatedBlocks.class)
        .addResult(locationSrc, newerFileInfo, HdfsFileStatus.class)
        .addResult(locationDst, newerFileInfo, HdfsFileStatus.class)
        .evaluate()
        .assertIncludes(locationDst)
        .assertExcludes(locationSrc)
        .assertInvoked(locationSrc, locationDst);

    verify(rpcClientMock, times(1)).invokeConcurrent(anyList(),
        any(RemoteMethod.class), anyBoolean(), anyLong(),
        eq(LocatedBlocks.class));
    verify(rpcClientMock, times(1)).invokeConcurrent(anyList(),
        any(RemoteMethod.class), anyBoolean(), anyLong(),
        eq(HdfsFileStatus.class));
  }
  
  private static class TestHelper {
    final Multimap<Class<?>, RemoteResult<RemoteLocation, ?>> resultsMap;
    final List<RemoteLocation> invokedLocations;
    PathLocation destination;

    TestHelper() {
      resultsMap = ArrayListMultimap.create();
      invokedLocations = new ArrayList<>();
    }

    <T> TestHelper addResult(RemoteLocation location, T object,
        Class<T> clazz) {
      resultsMap.put(clazz, new RemoteResult<>(location, object));
      return this;
    }
    
    TestHelper evaluate() throws IOException {
      return evaluate(path);
    }
    
    void injectMocks() throws IOException {
      // Mock the RPC client to return the results
      for (Class<?> clazz : resultsMap.keySet()) {
        List<RemoteResult<RemoteLocation, ?>> results =
            new ArrayList<>(resultsMap.get(clazz));
        doReturn(results).when(rpcClientMock)
            .invokeConcurrent(anyList(), any(RemoteMethod.class), anyBoolean(),
                anyLong(), eq(clazz));
      }
    }
    
    TestHelper evaluate(String path) throws IOException {
      injectMocks();

      // Set up a captor to capture the locations that are invoked
      @SuppressWarnings("unchecked")
      ArgumentCaptor<List<RemoteLocation>> captor =
          ArgumentCaptor.forClass(List.class);
      try {
        destination = resolver.getDestinationForPath(path);
      } finally {
        // Capture the locations that are invoked regardless of invocations
        verify(rpcClientMock, atLeast(0)).invokeConcurrent(captor.capture(),
            any(RemoteMethod.class), anyBoolean(), anyLong(), any());
        try {
          invokedLocations.addAll(captor.getValue());
        } catch (MockitoException e) {
          // Ignore and default to no invoked locations
        }
      }
      return this;
    }
    
    TestHelper assertIncludes(RemoteLocation... locations) {
      for (RemoteLocation location : locations) {
        Assert.assertTrue(destination.getDestinations().contains(location));
      }
      return this;
    }
    
    TestHelper assertExcludes(RemoteLocation... locations) {
      for (RemoteLocation location : locations) {
        Assert.assertFalse(destination.getDestinations().contains(location));
      }
      return this;
    }
    
    TestHelper assertInvoked(RemoteLocation... locations) {
      for (RemoteLocation location : locations) {
        Assert.assertTrue(invokedLocations.contains(location));
      }
      return this;
    }
    
    TestHelper assertNotInvoked(RemoteLocation... locations) {
      for (RemoteLocation location : locations) {
        Assert.assertFalse(invokedLocations.contains(location));
      }
      return this;
    }
  }
}
