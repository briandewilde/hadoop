package org.apache.hadoop.hdfs.server.federation.router;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.IllegalMigrationException;
import org.apache.hadoop.hdfs.server.federation.resolver.MigratingMountPointInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.MigratingMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;
import static org.junit.Assert.*;


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
}
