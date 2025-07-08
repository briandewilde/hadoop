package org.apache.hadoop.hdfs.server.federation.router;

import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.IllegalMigrationException;
import org.apache.hadoop.hdfs.server.federation.resolver.MigratingMountPointInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.MigratingMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;


/**
 * Tests router rpc with migrating mount table resolver.
 */
public class TestRouterRpcMigrationBehavior {
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver resolver;
  private static DistributedFileSystem nnFs0;
  private static DistributedFileSystem nnFs1;
  private static DistributedFileSystem routerFs;
  private static final Path sourcePath = new Path("/mp0");

  @BeforeClass
  public static void setUp() throws Exception {
    // Build and start a federated cluster with 2 namespaces
    cluster = new StateStoreDFSCluster(false, 2,
        MigratingMountTableResolver.class);
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().quota().rpc().build();
    // Set to 1 handler thread to expose problems with thread local variables
    routerConf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, 1);

    Configuration hdfsConf = new Configuration(false);
    hdfsConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

    cluster.addRouterOverrides(routerConf);
    cluster.addNamenodeOverrides(hdfsConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    resolver =
        (MountTableResolver) routerContext.getRouter().getSubclusterResolver();
    nnFs0 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(0), null).getFileSystem();
    nnFs1 = (DistributedFileSystem) cluster
        .getNamenode(cluster.getNameservices().get(1), null).getFileSystem();
    routerFs = (DistributedFileSystem) routerContext.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void resetTestEnvironment() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    RemoveMountTableEntryRequest request =
        RemoveMountTableEntryRequest.newInstance(sourcePath.toString());
    mountTableManager.removeMountTableEntry(request);
    nnFs0.delete(sourcePath, true);
    nnFs1.delete(sourcePath, true);
    MigratingMountTableResolver resolver =
        (MigratingMountTableResolver) routerContext.getRouter()
            .getSubclusterResolver();
  }
  
  private MountTable setupMountTable() throws IOException {
    Map<String, String> destMap = new HashMap<>();
    destMap.put("ns0", sourcePath.toString());
    nnFs0.mkdirs(sourcePath);
    MountTable entry = MountTable.newInstance(sourcePath.toString(), destMap);
    assertTrue(updateMountTable(entry));
    return entry;
  }
  
  private MountTable setupMountTableForMigration() throws IOException {
    MountTable entry = setupMountTable();
    entry.setMigratingMountPointInfo(
        new MigratingMountPointInfo("ns0", "ns1"));
    entry.addDestination("ns1", sourcePath.toString());
    nnFs1.mkdirs(sourcePath);
    assertTrue(updateMountTable(entry));
    return entry;
  }

  /**
   * Test that get file status (for a file) reads from the correct namespace,
   * where the file has a newer mod time.
   */
  @Test
  public void testFileStatusGetsLatestFile() throws IOException {
    setupMountTableForMigration();

    // Create the same file on nn0 and nn1, make sure nn1 is newer
    // Router should use nn1 to read the file
    Path filePath = new Path(sourcePath, "file");
    DFSTestUtil.createFile(nnFs0, filePath, 100L, (short) 1, 1024L);
    long file0ModTime = System.currentTimeMillis() - 1000;
    nnFs0.setTimes(filePath, file0ModTime, file0ModTime);
    DFSTestUtil.createFile(nnFs1, filePath, 100L, (short) 1, 1024L);
    long file1ModTime = System.currentTimeMillis();
    nnFs1.setTimes(filePath, file1ModTime, file1ModTime);
    assertTrue(nnFs0.exists(filePath));
    assertTrue(nnFs1.exists(filePath));
    assertTrue(routerFs.exists(filePath));
    assertEquals(
        routerFs.getFileStatus(filePath).getModificationTime(),
        nnFs1.getFileStatus(filePath).getModificationTime());

    // Reset the file on nn0 to have a newer mod time
    // Router should use nn0 to read the file
    long newFile0ModTime = System.currentTimeMillis();
    nnFs0.setTimes(filePath, newFile0ModTime, newFile0ModTime);
    assertEquals(
        routerFs.getFileStatus(filePath).getModificationTime(),
        nnFs0.getFileStatus(filePath).getModificationTime());

    // Create new files on one namespace only and verify router can read from
    // the correct namespace
    Path fileNs0 = new Path(sourcePath, "fileNs0");
    DFSTestUtil.createFile(nnFs0, fileNs0, 100L, (short) 1, 1024L);
    assertTrue(routerFs.exists(fileNs0));
    assertEquals(
        routerFs.getFileStatus(fileNs0).getModificationTime(),
        nnFs0.getFileStatus(fileNs0).getModificationTime());

    Path fileNs1 = new Path(sourcePath, "fileNs1");
    DFSTestUtil.createFile(nnFs1, fileNs1, 100L, (short) 1, 1024L);
    assertTrue(routerFs.exists(fileNs1));
    assertEquals(
        routerFs.getFileStatus(fileNs1).getModificationTime(),
        nnFs1.getFileStatus(fileNs1).getModificationTime());
  }

  /**
   * Test a create request should create files on target sub-cluster only with
   * the migrating mount table resolver.
   */
  @Test
  public void testCreateWritesDestination() throws IOException {
    setupMountTableForMigration();

    Path filePath = new Path(sourcePath, "file");
    DFSTestUtil.createFile(routerFs, filePath, 100L, (short) 1,
        1024L);
    assertTrue(nnFs1.exists(filePath));
    assertTrue(routerFs.exists(filePath));
    assertFalse(nnFs0.exists(filePath));
  }

  /**
   * Test that the createFileWithoutParents method does not create missing
   * parent directories (when not migrating). If this fails, other unit tests
   * that invoke createFileWithoutParents may incorrectly pass.
   * @throws IOException If an error occurs
   */
  @Test
  public void testCreateWithMissingDstDirsFailsWhenNotMigrating()
      throws IOException {
    setupMountTable();

    Path parent = new Path(sourcePath, "dir");
    assertFalse(routerFs.exists(parent));
    assertThrows(FileNotFoundException.class,
        () -> createFileWithoutParents(routerFs,
            new Path(parent, "file").toString()));
  }
  
  /**
   * Test a create request should create files on target sub-cluster only with
   * the migrating mount table resolver and missing directories.
   */
  @Test
  public void testCreateWithMissingDstDirs() throws IOException {
    setupMountTableForMigration();

    Path parent = new Path(sourcePath, "dir");
    Path path = new Path(parent, "file");
    nnFs0.mkdirs(parent);
    assertTrue(nnFs0.exists(parent));
    assertFalse(nnFs1.exists(parent));
    assertTrue(routerFs.exists(parent));
    
    // Set attributes on source directory
    nnFs0.setOwner(parent, "user", "group");
    // Use an unusual permission to ensure it is set on the destination
    nnFs0.setPermission(parent, FsPermission.valueOf("-rw-rw-rwx"));
    nnFs0.setXAttr(parent, "user.a1", "v1".getBytes());
    
    // Create file on destination to trigger parent directory creation
    createFileWithoutParents(routerFs, path.toString());

    assertEquals("user", nnFs1.getFileStatus(parent).getOwner());
    assertEquals("group", nnFs1.getFileStatus(parent).getGroup());
    assertEquals(FsPermission.valueOf("-rw-rw-rwx"),
        nnFs1.getFileStatus(parent).getPermission());
    assertEquals("v1", new String(nnFs1.getXAttr(parent, "user.a1")));
  }

  /**
   * Test a create request should create files on target sub-cluster only with
   * the migrating mount table resolver and nested missing directories.
   */
  @Test
  public void testCreateWithNestedMissingDstDirs() throws IOException {
    setupMountTableForMigration();

    Path parent = new Path(sourcePath, "top/dir");
    Path path = new Path(parent, "file");
    nnFs0.mkdirs(parent);
    assertTrue(nnFs0.exists(parent));
    assertFalse(nnFs1.exists(parent));
    assertTrue(routerFs.exists(parent));

    createFileWithoutParents(routerFs, path.toString());

    assertTrue(nnFs1.exists(parent));
    
    // Verify the permissions match
    assertPermissionsMatch(nnFs0.getFileStatus(parent),
        nnFs1.getFileStatus(parent));
    
    // Verify that all op-level tmp objects are deleted
    Path tmp = ((MigratingMountTableResolver) resolver).getMountPointTempPrefix(
        sourcePath);
    assertEquals(0, nnFs1.listStatus(tmp).length);
  }

  /**
   * Test a create request where the createParent flag is true to ensure
   * that the migration copied the parents with the correct metadata.
   */
  @Test
  public void testCreateWithCreateParentFlag() throws IOException {
    setupMountTableForMigration();

    // /mp0/top/dir/sub will be created by the namenode, as it is not already
    // present on the src; ensure that /mp0/top and /mp0/top/dir are not also
    // created by the namenode but instead copied from the src
    Path parent = new Path(sourcePath, "top/dir");
    Path path = new Path(parent, "sub/file");
    nnFs0.mkdirs(parent);
    assertTrue(nnFs0.exists(parent));
    assertFalse(nnFs1.exists(parent));
    assertTrue(routerFs.exists(parent));

    // Set unusual permissions to ensure dir metadata is copied to destination
    for (Path p = parent; !sourcePath.equals(p); p = p.getParent()) {
      nnFs0.setPermission(p, FsPermission.valueOf("-rw-rw-rwx"));
    }

    routerFs.create(path);

    // Assert the path exists on the dst
    assertTrue(nnFs1.exists(path));

    // The namenode should only create the parents on the dst
    assertFalse(nnFs0.exists(path));
    
    // Verify unusual permissions match to ensure dir metadata was copied
    for (Path p = parent; !sourcePath.equals(p); p = p.getParent()) {
      assertEquals(FsPermission.valueOf("-rw-rw-rwx"),
          nnFs1.getFileStatus(p).getPermission());
    }

    // Verify that all op-level tmp objects are deleted
    Path tmp = ((MigratingMountTableResolver) resolver).getMountPointTempPrefix(
        sourcePath);
    assertEquals(0, nnFs1.listStatus(tmp).length);
  }
  
  @Ignore
  @Test
  public void testCreateWithMissingDstDirsPerformance()
      throws IOException, IllegalAccessException {
    setupMountTableForMigration();
    
    Path ancestor1 = new Path(sourcePath, "ancestor1");
    Path ancestor2 = new Path(ancestor1, "ancestor2");
    Path parent1 = new Path(ancestor2, "parent1");
    Path child1 = new Path(parent1, "child1");
    Path parent2 = new Path(ancestor2, "parent2");
    Path child2 = new Path(parent2, "child2");

    // Create all directories on source
    nnFs0.mkdirs(child1);
    nnFs0.mkdirs(child2);
    
    try (MigrationClientSpy spy = new MigrationClientSpy(
        cluster)) {
      // Create a file in a deeply nested directory
      routerFs.mkdirs(new Path(child1, "subdir"));
      routerFs.mkdirs(new Path(child2, "subdir"));
      
      // Verify that file status is invoked once each on source and destination
      // for the first child directory
      spy.verifyBatchInvocation("ns0", child1, HdfsFileStatus.class, 1);
      spy.verifyBatchInvocation("ns1", child1, HdfsFileStatus.class, 1);

      // Verify that the file status is not invoked (short-circuited after
      // ancestors) for the first parent directory
      spy.verifyBatchInvocation("ns0", parent1, HdfsFileStatus.class, 0);
      spy.verifyBatchInvocation("ns1", parent1, HdfsFileStatus.class, 0);
      
      // Verify that the file status is invoked once only on the destination
      // for the ancestor directories
      // (It will not be invoked on ancestor2, due to short-circuit)
      spy.verifyBatchInvocation("ns0", ancestor1, HdfsFileStatus.class, 0);
      spy.verifyBatchInvocation("ns1", ancestor1, HdfsFileStatus.class, 1);
      
      // Verify that rename is invoked exactly twice, once for /mp0/ancestor1
      // and once for /mp0/ancestor1/ancestor2/parent2
      Mockito.verify(spy.rpcServerSpy, Mockito.times(2))
          .rename2(any(), any(), any());
      Mockito.verify(spy.rpcServerSpy)
          .rename2(any(), eq("/mp0/ancestor1"), any());
      Mockito.verify(spy.rpcServerSpy)
          .rename2(any(), eq("/mp0/ancestor1/ancestor2/parent2"), any());
    }
  }

  /**
   * Test that get file status (for a directory) reads from the destination
   * namespace, whenever available.
   */
  @Test
  public void testFileStatusPrefersDstDirs() throws IOException {
    setupMountTableForMigration();

    // The same path will be created on src and/or dst with different
    // permissions to determine which is read
    Path path = new Path(sourcePath, "dir");
    FsPermission srcPermission = new FsPermission((short)00700);
    FsPermission dstPermission = new FsPermission((short)00755);

    // If the dir only exists on src, router should use src
    nnFs0.mkdirs(path, srcPermission);
    assertTrue(routerFs.exists(path));
    assertEquals(srcPermission, routerFs.getFileStatus(path).getPermission());

    // If the dir exists on src and dst, router should use dst
    nnFs1.mkdirs(path, dstPermission);
    assertTrue(routerFs.exists(path));
    assertEquals(dstPermission, routerFs.getFileStatus(path).getPermission());
    
    // Delete the dir on the src and verify that the router still uses the dst
    nnFs0.delete(path, false);
    assertTrue(routerFs.exists(path));
    assertEquals(dstPermission, routerFs.getFileStatus(path).getPermission());

    // Recreate the dir on the src and verify that the router still uses the dst
    nnFs0.mkdirs(path, srcPermission);
    assertTrue(routerFs.exists(path));
    assertEquals(dstPermission, routerFs.getFileStatus(path).getPermission());
  }

  /**
   * Test that listing a migrating directory shows files from both source and
   * destination
   */
  @Test
  public void testDirectoryListingGetsUnion() throws IOException {
    setupMountTableForMigration();

    Path path0 = new Path(sourcePath, "file0");
    nnFs0.createNewFile(path0);

    Path path1 = new Path(sourcePath, "file1");
    nnFs1.createNewFile(path1);

    assertTrue(routerFs.exists(path0));
    assertTrue(routerFs.exists(path1));
    List<FileStatus> fileStatuses =
        Arrays.asList(routerFs.listStatus(sourcePath));
    assertEquals(2, fileStatuses.size());

    String result0 = fileStatuses.get(0).getPath().toUri().getPath();
    String result1 = fileStatuses.get(1).getPath().toUri().getPath();
    assertTrue(
        result0.equals(path0.toString()) || result0.equals(path1.toString()));
    assertTrue(
        result1.equals(path0.toString()) || result1.equals(path1.toString()));
  }

  /**
   * Test that listing a migrating directory shows only the file with the newest
   * mod time
   */
  @Test
  public void testDirectoryListingUnionDeduplicatesOnModTime()
      throws IOException {
    setupMountTableForMigration();

    Path path = new Path(sourcePath, "file");
    nnFs0.createNewFile(path);

    nnFs1.createNewFile(path);

    assertTrue(routerFs.exists(path));
    List<FileStatus> fileStatuses =
        Arrays.asList(routerFs.listStatus(sourcePath));
    assertEquals(1, fileStatuses.size());
    
    // Assert that the file returned is from nnFs1 (the latest mod time)
    assertEquals(nnFs1.getFileStatus(path).getModificationTime(),
        routerFs.getFileStatus(path).getModificationTime());
    
    // Update the file on nn0 to have a newer mod time
    nnFs0.setTimes(path, System.currentTimeMillis(), -1);
    
    // Assert that the file returned is from nnFs0 (the latest mod time)
    assertEquals(nnFs0.getFileStatus(path).getModificationTime(),
        routerFs.getFileStatus(path).getModificationTime());
  }

  /**
   * Test that reading a single file reads from the correct namespace, where the
   * file has the same mod time.
   */
  @Test
  public void testReadGetsLatestFile() throws IOException {
    setupMountTableForMigration();

    // Create the file on src and ensure it is read
    Path path = new Path(sourcePath, "file");
    writeLine(nnFs0.create(path), "Source file");
    assertEquals(readLine(nnFs0.open(path)), readLine(routerFs.open(path)));

    // Create the same file on dst and ensure it is read
    writeLine(nnFs1.create(path), "Destination file");
    assertEquals(readLine(nnFs1.open(path)), readLine(routerFs.open(path)));

    // Update the mod time on the src file and ensure it is read
    nnFs0.setTimes(path, System.currentTimeMillis(), -1);
    assertEquals(readLine(nnFs0.open(path)), readLine(routerFs.open(path)));
  }

  @Test
  public void testCreateFileBeforeMigrationGoesToSrc() throws IOException {
    setupMountTable();
    Path path = new Path(sourcePath, "file");
    String str = "Source file";

    // Create the file before the migration
    FSDataOutputStream out = routerFs.create(path);
    // Start migration and then write to the file
    setupMountTableForMigration();
    try (BufferedWriter bufferedWriter = new BufferedWriter(
        new java.io.OutputStreamWriter(out, StandardCharsets.UTF_8))) {
      bufferedWriter.write(str.split("\n")[0]);
    }

    // Ensure it was created and written on the source
    assertTrue(nnFs0.exists(path));
    assertFalse(nnFs1.exists(path));
    assertEquals(str, readLine(nnFs0.open(path)));
  }

  @Test
  public void testCreateFileDuringMigrationGoesToDst() throws IOException {
    setupMountTableForMigration();
    Path path = new Path(sourcePath, "file");
    String str = "Destination file";

    // Write the file during the migration
    writeLine(routerFs.create(path), str);

    // Ensure it was created and written on the destination
    assertTrue(nnFs1.exists(path));
    assertFalse(nnFs0.exists(path));
    assertEquals(str, readLine(nnFs1.open(path)));
  }
  
  @Test
  public void testCreateFilePresentOnSrcThrows() throws IOException {
    setupMountTableForMigration();
    Path path = new Path(sourcePath, "file");

    // Create the file on src
    writeLine(nnFs0.create(path), "Source file");

    // Assert that the file is not on the dst
    assertFalse(nnFs1.exists(path));

    // Try to create the same file on dst and ensure it throws an exception
    RemoteException e = assertThrows(RemoteException.class, () -> 
      writeLine(routerFs.create(path), "Destination file"));
    assertEquals(IllegalMigrationException.class.getName(), e.getClassName());
  }
  
  @Test
  public void testMkdirsDirPresentOnSrc() throws IOException {
    setupMountTableForMigration();
    Path path = new Path(sourcePath, "file");

    FsPermission unusualPermission = FsPermission.valueOf("-r---w---x")
        .applyUMask(FsPermission.getUMask(routerContext.getConf()));

    // Assert the unusual permissions are not the default; if the dir default
    // changes so that ths fails, the unusual permissions must also change
    assertNotEquals(FsPermission.getDirDefault().applyUMask(
        FsPermission.getUMask(routerContext.getConf())), unusualPermission);

    // Create dir on src using unusual permissions
    assertTrue(nnFs0.mkdirs(path, unusualPermission));

    // Create dir on src again using default permission
    assertTrue(nnFs0.mkdirs(path));

    // Assert that the dir on the src kept the unusual permissions
    assertEquals(unusualPermission, nnFs0.getFileStatus(path).getPermission());
    
    // Assert that the dir is not on the dst
    assertFalse(nnFs1.exists(path));

    // Create the same dir on the router with default permissions
    assertTrue(routerFs.mkdirs(path));
    
    // Ensure the dir was created on the dst with the unusual permissions (same
    // permissions as the src)
    assertTrue(nnFs1.exists(path));
    assertEquals(unusualPermission, nnFs1.getFileStatus(path).getPermission());
  }

  /**
   * A helper method to write a simple, one-line string to a file.
   * @param out The output stream to write to
   * @param str The string to write to the file
   * @throws IOException If an error occurs
   */
  private void writeLine(FSDataOutputStream out, String str)
      throws IOException {
    try (BufferedWriter bufferedWriter = new BufferedWriter(
        new java.io.OutputStreamWriter(out, StandardCharsets.UTF_8))) {
      bufferedWriter.write(str.split("\n")[0]);
    }
  }

  /**
   * A helper method to read a simple, one-line string from a file.
   * @param in The input stream to read from
   * @return The string read from the file
   * @throws IOException If an error occurs
   */
  private String readLine(FSDataInputStream in) throws IOException {
    try (BufferedReader bufferedReader = new BufferedReader(
        new java.io.InputStreamReader(in, StandardCharsets.UTF_8))) {
      return bufferedReader.readLine();
    }
  }

  /**
     * Add a mount table entry to the mount table through the admin API.
     * @param entry Mount table entry to add.
     * @return If it was successfully added.
     * @throws IOException If an error occurs
     */
  private boolean updateMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    UpdateMountTableEntryRequest request =
        UpdateMountTableEntryRequest.newInstance(entry);
    UpdateMountTableEntryResponse response =
        mountTableManager.updateMountTableEntry(request);

    // Reload the Router cache
    resolver.loadCache(true);

    return response.getStatus();
  }
  
  /**
   * This creates a file but does not create missing parent directories
   * @param dfs The distributed filesystem to use
   * @param path The path to create
   * @throws IOException If an error occurs
   */
  private void createFileWithoutParents(DistributedFileSystem dfs, String path)
      throws IOException {
    try (DFSOutputStream out = dfs.getClient()
        .create(path, FsPermission.valueOf("-rwxrwxrwx"),
            EnumSet.of(CreateFlag.CREATE), false, (short) 1, 1024, null, 1024,
            null, null)) {
      out.write('a');
      out.hflush();
    }
  }

  /**
   * Assert that the permissions of the expected and actual file status
   * @param expected The expected file status
   * @param actual The actual file status
   */
  private void assertPermissionsMatch(FileStatus expected, FileStatus actual) {
    assertEquals(expected.getOwner(), actual.getOwner());
    assertEquals(expected.getGroup(), actual.getGroup());
    assertEquals(expected.getPermission(), actual.getPermission());
  }

  /**
   * An auto-closeable class that replaces the MigratingMountTableResolver's
   * RouterRpcServer and RouterRpcClient with spies and provides convenience
   * methods for verifying batch invocations.
   */
  static class MigrationClientSpy implements AutoCloseable {
    private final Map<MiniRouterDFSCluster.RouterContext, RouterRpcServer>
        rpcServerOrigMap = new HashMap<>();
    // For this test, both routers will share the same client
    private final RouterRpcServer rpcServerSpy;
    private final RouterRpcClient rpcClientSpy;
    public MigrationClientSpy(StateStoreDFSCluster cluster) {
      rpcClientSpy =
          Mockito.spy(cluster.getRandomRouter().getRouterRpcClient());
      rpcServerSpy = Mockito.spy(routerContext.getRouterRpcServer());
      Mockito.doReturn(rpcClientSpy).when(rpcServerSpy)
          .getRPCClient();
      for (MiniRouterDFSCluster.RouterContext router : cluster.getRouters()) {
        rpcServerOrigMap.put(router, router.getRouterRpcServer());
        ((MigratingMountTableResolver) router.getRouterRpcServer()
            .getSubclusterResolver()).setRpcServer(rpcServerSpy);
      }
    }

    public void verifyBatchInvocation(String nsId, Path path, Class<?> clazz,
        int times) throws IOException {
      Mockito.verify(rpcClientSpy, Mockito.times(times))
          .invokeConcurrent(argThat(
              (List<RemoteLocation> locations) -> locations.stream()
                  .filter(location -> location.getNameserviceId().equals(nsId))
                  .anyMatch(
                      location -> location.getSrc().equals(path.toString()))),
              any(RemoteMethod.class), eq(false), eq(-1L), eq(clazz));
    }
    
    @Override
    public void close() throws IllegalAccessException {
      for (MiniRouterDFSCluster.RouterContext router :
          rpcServerOrigMap.keySet()) {
        ((MigratingMountTableResolver) router.getRouterRpcServer()
            .getSubclusterResolver()).setRpcServer(
            rpcServerOrigMap.get(router));
      }
    }
  } 
}
