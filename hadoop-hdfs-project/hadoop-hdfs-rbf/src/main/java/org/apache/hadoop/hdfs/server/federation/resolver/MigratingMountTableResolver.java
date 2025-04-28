package org.apache.hadoop.hdfs.server.federation.resolver;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteParam;
import org.apache.hadoop.hdfs.server.federation.router.RemoteResult;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;


/**
 * Handles mount point migration by determining the destination(s) for the given
 * path(s).
 */
public class MigratingMountTableResolver extends MountTableResolver {
  private static final Logger LOG =
      LoggerFactory.getLogger(MigratingMountTableResolver.class);
  private RouterRpcServer rpcServer = null;
  private RouterRpcClient rpcClient = null;

  /**
   * A wrapper for the current migration context, used to ensure operations
   * are handled consistently from beginning to end.
   */
  private final MigrationContextCache context;

  /**
   * A default batch size of 2 avoids excessive fan-out but still allows source
   * and destination block locations to be fetched concurrently.
   */
  private final int batchSize;

  /**
   * The nameservices configured in the cluster.
   */
  private static final Collection<String> nameServices = new HashSet<>();

  /**
   * A type comparator to ensure files and directories are not compared.
   * Throws an IllegalArgumentException if a file is compared to a directory.
   */
  private static final Comparator<HdfsFileStatus> typeComparator =
      Comparator.comparing(HdfsFileStatus::isDirectory, (i1, i2) -> {
        if (i1 != i2) {
          throw new IllegalArgumentException(
              "Cannot compare file to directory");
        } else {
          return 0;
        }
      });

  /**
   * A comparator to determine the latest HdfsFileStatus objects by mod time and
   * always return directories as equal.
   * Throws an IllegalArgumentException if a file is compared to a directory.
   */
  private static final Comparator<HdfsFileStatus> latestComparator =
      Comparator.nullsFirst(typeComparator.thenComparing(
          i -> i.isDirectory() ? 0 : i.getModificationTime()));

  public MigratingMountTableResolver(Configuration conf, Router routerService) {
    super(conf, routerService);

    nameServices.addAll(conf.getTrimmedStringCollection(DFS_NAMESERVICES));
    batchSize = conf.getInt(DFS_ROUTER_MIGRATION_BATCH_SIZE, 2);
    context = new MigrationContextCache();
  }

  /**
   * Set the RPC server for the resolver. This also saves a reference to the RPC
   * server's client. It should be called when the router is initialized.
   * @param rpcServer The RPC server to be used by the resolver
   */
  public void setRpcServer(RouterRpcServer rpcServer) {
    this.rpcServer = rpcServer;
    this.rpcClient = rpcServer.getRPCClient();
  }

  /**
   * Set the behavior of the current operation during mount point migration.
   */
  public enum MigrationBehavior {
    /**
     * Only allow the operation on the destination mount point
     */
    DST_ONLY,
    /**
     * Consider both the source and destination for metadata-only operations
     */
    UNION,
    /**
     * Allow the operation on the source or destination with the latest mod
     * time, or the destination if the mod times are equal; for directories, the
     * destination used whenever it exists.
     */
    LATEST,
    /**
     * Allow the operation on the source or destination with the lease.
     * This is needed because in-flight writes to the source may call operations
     * (e.g. addBlock) that must be sent to the source even when mod times are
     * equal; however during migration, data pipeline operations (e.g. addBlock)
     * are generally sent to the destination. This can be replaced to LATEST if
     * all in-flight writes to the source are completed before files are copied
     * to the destination.
     */
    LEASED,
    /**
     * Migration behavior is not defined; operations will fail unless set
     */
    UNDEFINED
  }

  /**
   * Set the behavior of the current operation during migration and save
   * this to a context which also includes the current MigratingMountPointInfo.
   * The context is saved at the beginning of the operation to ensure that
   * handling is consistent throughout, since an operation during migration may
   * result in multiple calls to the namenode (e.g. to determine the latest
   * mod time before reading). This handles the non-atomic nature of operations
   * during migration without requiring a lock for IO on the mount table.
   * <p>
   * This context persists during the current operation and does not need to be
   * cleared. It is saved in an LRU cache proportional to the number of handler
   * threads, using the RPC call id as the key, to ensure that the context
   * resets to a default value when the handler thread is reused. Therefore,
   * only operations which support migration should set the migration behavior.
   * 
   * @param migrationBehavior the behavior which determines namespace(s) to use
   * @param path if specified, the path for which the migration behavior is set
   * @throws IOException If an error occurs
   */
  public void setMigrationBehavior(MigrationBehavior migrationBehavior,
      @Nullable String path)
      throws IOException {
    if (rpcClient == null || rpcServer == null) {
      throw new IllegalMigrationException(
          "MigratingMountTableResolver not initialized");
    } else {
      // Use the migrating mount point info saved to the context cache
      MigratingMountPointInfo migratingMountPointInfo =
          context.set(migrationBehavior, path).getMigratingMountPointInfo();
      if (migratingMountPointInfo != null) {
        // Record the mount point migration and behavior
        LOG.info("Using migration behavior {} ({}->{}) on {}; call id {}",
            migrationBehavior, migratingMountPointInfo.getSrcNs(),
            migratingMountPointInfo.getDstNs(), path, RPC.Server.getCallId());
      }
    }
  }

  /**
   * Check if either of the mount tables is migrating.
   * @param newEntry optional new mount table entry.
   * @param oldEntry optional old mount table entry.
   * @return true if either of the mount tables is migrating
   */
  public static boolean isMigrating(@Nullable MountTable newEntry,
      @Nullable MountTable oldEntry) {
    MigratingMountPointInfo newMigrationInfo =
        newEntry == null ? null : newEntry.getMigratingMountPointInfo();
    MigratingMountPointInfo oldMigrationInfo =
        oldEntry == null ? null : oldEntry.getMigratingMountPointInfo();
    return newMigrationInfo != null || oldMigrationInfo != null;
  }

  /**
   * Reconcile the mount table entry with the nameservices and locations of the
   * migration. When not migrating, the mount table entry is expected to have
   * only a single nameservice; but when migrating, the mount table entry must
   * have locations for both the source and destination nameservices. This
   * reconciles the mount table entry to have the correct nameservices and
   * corresponding locations.
   * If the entry cannot be reconciled correctly, an exception is thrown. This
   * is intended for RouterAdmin when adding or updating a mount table entry.
   * @param newEntry new mount table entry.
   * @param oldEntry optional old mount table entry.
   * @return a reference to the new mount table entry, which may be modified.
   * @throws IllegalMigrationException if the mount table entry cannot be
   *         reconciled.
   */
  public static MountTable reconcileEntryWithMigration(
      MountTable newEntry, @Nullable MountTable oldEntry)
      throws IllegalMigrationException {
    // The MigratingMountPointInfo that is being set; if not set, this
    // implies a migration is being removed.
    MigratingMountPointInfo newMigrationInfo =
        newEntry.getMigratingMountPointInfo();
    // The MigratingMountPointInfo that was already set; if not set, this
    // implies a migration is being added.
    MigratingMountPointInfo oldMigrationInfo =
        oldEntry == null ? null : oldEntry.getMigratingMountPointInfo();

    // Only reconcile if mount table is already migrating or is about to start
    // migrating; if neither, this is a no-op.
    if (newMigrationInfo != null || oldMigrationInfo != null) {
      Set<String> newNsIds = newEntry.getDestinations().stream()
          .map(RemoteLocation::getNameserviceId)
          .collect(Collectors.toSet());

      // Determine the source and destination namespaces from the new migration
      // info if possible, else use the old migration info, else null
      String srcNsId = newMigrationInfo != null ?
          newMigrationInfo.getSrcNs() : oldMigrationInfo.getSrcNs();
      String dstNsId = newMigrationInfo != null ?
          newMigrationInfo.getDstNs() : oldMigrationInfo.getDstNs();

      // The source and destination can never be the same
      if (srcNsId.equals(dstNsId)) {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table cannot have the same source and destination (%s->%s) for"
            + " %s", srcNsId, dstNsId, newEntry.getSourcePath()));
      }

      // The mount table can only use namespaces that are migrating
      if (!Arrays.asList(srcNsId, dstNsId).containsAll(newNsIds)) {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table must use only namespaces used in migration (%s->%s), but"
            + " encountered %s for %s", srcNsId, dstNsId, newNsIds,
            newEntry.getSourcePath()));
      }

      if (newMigrationInfo != null && oldMigrationInfo == null) {
        // Add migration info, since the mount point was not already migrating
        if (!newNsIds.contains(srcNsId)) {
          throw new IllegalMigrationException(String.format("Migrating mount"
              + " table must have the source namespace, but encountered %s for"
              + " %s", newNsIds, newEntry.getSourcePath()));
        }
        if (newNsIds.size() == 1) {
          // If there is only one namespace, add the destination
          newNsIds.add(dstNsId);
        }
        LOG.info("Adding migration info ({} => {}->{}) for {}",
            srcNsId, srcNsId, dstNsId, newEntry.getSourcePath());
      } else if (newMigrationInfo == null) {
        // Removing migration info, since the new mount point is not migrating
        if (newNsIds.size() > 1) {
          // If there are two namespaces, keep only the source
          newNsIds.remove(dstNsId);
        }
        String newNsId = newNsIds.iterator().next();
        LOG.info("Removing migration info ({}->{} => {}) for {}",
            srcNsId, dstNsId, newNsId, newEntry.getSourcePath());
      } else {
        // Updating migration info, since the new mount point is migrating and
        // the old mount point was already migrating
        if (!newNsIds.contains(srcNsId)) {
          throw new IllegalMigrationException(String.format("Migrating mount"
              + " table must have the source namespace, but encountered %s for"
              + " %s", newNsIds, newEntry.getSourcePath()));
        }
        if (newNsIds.size() == 1) {
          // If there is only one namespace, add the destination
          newNsIds.add(dstNsId);
        }
        // Ensure that any changes to the migration info are valid
        verifyMigrationUpdate(newMigrationInfo, oldMigrationInfo,
            newEntry.getSourcePath());
      }

      if (newNsIds.isEmpty()) {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table must have at least one destination, but encountered none"
            + " for %s", newEntry.getSourcePath()));
      } else {
        reconcileEntryLocations(newEntry, newNsIds);
      }
    }
    return newEntry;
  }

  /**
   * During an active migration, ensure that migration updates are supported.
   * This serves as a check to ensure migrations update occur only in a
   * controlled way (e.g. a rollback). This does not handle cases where a
   * migration is added or removed.
   * @param newMigrationInfo the new migration info
   * @param oldMigrationInfo the old migration info
   * @param sourcePath the source path
   * @throws IllegalMigrationException if the migration update is illegal
   */
  private static void verifyMigrationUpdate(
      MigratingMountPointInfo newMigrationInfo,
      MigratingMountPointInfo oldMigrationInfo, String sourcePath)
      throws IllegalMigrationException {
    // No-op if there is no difference between the new and old migration info
    if (!newMigrationInfo.equals(oldMigrationInfo)) {
      String newSrcNs = newMigrationInfo.getSrcNs();
      String newDstNs = newMigrationInfo.getDstNs();
      String oldSrcNs = oldMigrationInfo.getSrcNs();
      String oldDstNs = oldMigrationInfo.getDstNs();
      // At present, if a mount point is already migrating, it can only
      // transition to/from a rollback
      if (newSrcNs.equals(oldDstNs) && newDstNs.equals(oldSrcNs)) {
        LOG.info("Reversing migration ({}->{} => {}->{}) for {}", oldSrcNs,
            oldDstNs, newSrcNs, newDstNs, sourcePath);
      } else {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table encountered illegal transition (%s->%s => %s->%s) for %s",
            oldSrcNs, oldDstNs, newSrcNs, newDstNs, sourcePath));
      }
    }
  }

  /**
   * Reconcile the entry locations to be consistent with the nameservices.
   * This ensures that the newEntry has locations corresponding to all
   * nameservices specified in nsIds and no other nameservices. A migration can
   * be entered by adding the destination nameservices to nsIds and exited by
   * removing the old nameservices from nsIds.
   * @param newEntry the new mount table entry
   * @param nsIds the namespaces in the migration
   * @throws IllegalMigrationException if the locations are inconsistent with
   *         the migration
   */
  private static void reconcileEntryLocations(MountTable newEntry,
      Set<String> nsIds) throws IllegalMigrationException {
    // If there are no configured namespaces, migration is disabled in config;
    // skip the check but allow manipulation of the mount table.
    if (!nameServices.isEmpty() && !nameServices.containsAll(nsIds)) {
      throw new IllegalMigrationException(String.format("Migrating mount table"
          + " must use configured namespaces, but encountered %s for %s", nsIds,
          newEntry.getSourcePath()));
    }

    // Add and remove namespaces to match migration
    newEntry.setDestinations(newEntry.getDestinations().stream()
        .filter(location -> nsIds.contains(location.getNameserviceId()))
        .flatMap(location -> nsIds.stream()
            .map(nsId -> new RemoteLocation(nsId, location.getDest(),
                location.getSrc())))
        .distinct()
        .collect(Collectors.toList()));
  }
  
  /**
   * Get the destinations for a global path considering active migrations.
   * During a migration, this method is overridden to provide the destination(s)
   * based the set migration behavior (or throws an exception if no migration
   * behavior was set); otherwise, it falls back to the super method. Results
   * are from the mount table cache. Multiple destinations may be returned for
   * select metadata-only operations, such as UNION, which will consider both
   * the source and destination.
   * <p>
   * In all cases, the super method is called (or else an exception is thrown)
   * as part of this path to ensure super method handling, such as trash path,
   * is called. During migration, the super method called when the migration
   * context is first saved or else an exception is thrown; otherwise, it is
   * called here.
   * 
   * @param path Global path.
   * @return Location in a destination namespace or null if it does not exist.
   * @throws IOException Throws exception if the data is not available.
   */
  @Override
  public PathLocation getDestinationForPath(String path) throws IOException {
    if (context.getMigratingMountPointInfo(path) == null) {
      // There is no saved context if the mount point is not migrating or
      // if the migration behavior is not set.
      return super.getDestinationForPath(path);
    } else {
      MigrationPair<RemoteLocation> remoteLocations = getRemoteLocations(path);
      List<RemoteLocation> targetLocations;
      MigrationBehavior migrationBehavior = context.getMigrationBehavior(path);
      switch (migrationBehavior) {
        case DST_ONLY:
          targetLocations = Collections.singletonList(
              getDestinationLocation(remoteLocations));
          break;
        case UNION:
          targetLocations = remoteLocations.asList();
          break;
        case LATEST:
          targetLocations = Collections.singletonList(
              getComparatorLocation(latestComparator, remoteLocations));
          break;
        case LEASED:
          targetLocations = Collections.singletonList(
              getLeasedLocation(remoteLocations));
          break;
        case UNDEFINED:
          // Cause new operations to fail until the migration behavior is set
          throw new IOException(String.format("Operation has no defined"
              + " migration behavior; call id %s", RPC.Server.getCallId()));
        default:
          // Cause new migration behaviors to fail until defined
          throw new UnsupportedOperationException(String.format(
              "Migration behavior %s is not defined; call id %s",
              migrationBehavior, RPC.Server.getCallId()));
      }
      LOG.debug("Using {} migration destinations; call id {}",
          targetLocations.stream()
              .map(RemoteLocation::getNameserviceId)
              .collect(Collectors.joining(", ")), RPC.Server.getCallId());
      return new PathLocation(path, targetLocations);
    }
  }

  /**
   * Get source and destination remote locations as a pair for the given path.
   * @param path The current path
   * @return A pair of with the source location on the left and destination
   *         location on the right
   * @throws IOException If an error occurs while retrieving remote locations
   */
  private MigrationPair<RemoteLocation> getRemoteLocations(String path)
      throws IOException {
    MigratingMountPointInfo migratingMountPointInfo =
        context.getMigratingMountPointInfo(path);
    if (migratingMountPointInfo == null) {
      throw new IllegalMigrationException(String.format("Path %s is not on a"
          + " migrating mount point", path));
    }
    PathLocation pathLocation = getPathLocation(path);
    RemoteLocation srcLocation = null;
    RemoteLocation dstLocation = null;
    for (RemoteLocation loc : pathLocation.getDestinations()) {
      if (loc.getNameserviceId().equals(migratingMountPointInfo.getSrcNs())) {
        srcLocation = loc;
      } else if (loc.getNameserviceId()
          .equals(migratingMountPointInfo.getDstNs())) {
        dstLocation = loc;
      }
    }
    if (srcLocation == null) {
      throw new IllegalMigrationException(String.format("Migrating mount point"
          + " does not have a valid source; path %s, call id %s", path,
          RPC.Server.getCallId()));
    } else if (dstLocation == null) {
      throw new IllegalMigrationException(String.format("Migrating mount point"
          + " does not have a valid destination; path %s, call id %s", path,
          RPC.Server.getCallId()));
    }
    return MigrationPair.of(srcLocation, dstLocation);
  }

  /**
   * Get the location for the given path. This attempts to load the location
   * from the migration context but falls back to the super method, which
   * specifically ensures that ops complete as expected when a migrating mount
   * point succeeds (and the source or destination locations removed); in cases
   * where it fails, this should have no effect other than to allow in-flight
   * ops to continue to the source and/or destination consistently. (It will not
   * prevent failures to create missing destination directories, however there
   * should be no missing destination dirs if migration succeeded.)
   * <p>
   * This should generally replace calls to super.getDestinationForPath(path)
   * made from within MigratingMountTableResolver.
   * @param path The path for which the location should be retrieved
   * @return The location for the given path or null if it does not exist
   * @throws IOException If an error occurs while retrieving the location
   */
  private PathLocation getPathLocation(String path)
      throws IOException {
    PathLocation defaultLocation = context.getRemoteLocations(path);
    if (defaultLocation == null) {
      // This can occur if an operation tries to get path locations for a
      // different path (e.g. parent); it is not an error, but a cache miss.
      LOG.warn("Path {} is not in the migration context; call id {}",
          path, RPC.Server.getCallId());
      return super.getDestinationForPath(path);
    } else {
      return defaultLocation;
    }
  }

  /**
   * Get the location using a comparator, or the destination if the comparator
   * is equal
   * @param comparator The comparator to be used
   * @param locations The remote locations as a pair
   * @return The location with the larger comparator value
   * @throws IOException If an error occurs while retrieving file information
   */
  private RemoteLocation getComparatorLocation(
      Comparator<HdfsFileStatus> comparator,
      MigrationPair<RemoteLocation> locations) throws IOException {
    Map<RemoteLocation, HdfsFileStatus> fileInfo =
        getFileInfo(locations.asList());
    HdfsFileStatus srcFileInfo = fileInfo.get(locations.getSrc());
    HdfsFileStatus dstFileInfo = fileInfo.get(locations.getDst());

    // Return dst location if dst comparator is greater than or equal to src
    try {
      if (comparator.compare(srcFileInfo, dstFileInfo) <= 0) {
        return locations.getDst();
      } else {
        return locations.getSrc();
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalMigrationException(
          String.format("Cannot compare file to directory; path %s, call id %s",
              locations.getPath(), RPC.Server.getCallId()), e);
    }
  }

  /**
   * Get the destination location, ensuring that the destination is not
   * out-of-date
   * Note: Files copied to the destination while in-flight writes occur to
   * source may be out-of-date and cannot accept DST_ONLY operations without
   * potential data-loss. Directories are metadata-only therefore unaffected.
   * @param locations The remote locations as a pair
   * @return The destination location
   * @throws IOException If an error occurs while retrieving file information or
   *                     the destination is out-of-date
   */
  private RemoteLocation getDestinationLocation(
      MigrationPair<RemoteLocation> locations) throws IOException {
    Map<RemoteLocation, HdfsFileStatus> fileInfo =
        getFileInfo(locations.asList());
    HdfsFileStatus srcFileInfo = fileInfo.get(locations.getSrc());
    HdfsFileStatus dstFileInfo = fileInfo.get(locations.getDst());

    // Return dst location if src modtime is less than or equal to dst modtime
    try {
      if (latestComparator.compare(srcFileInfo, dstFileInfo) <= 0) {
        return locations.getDst();
      } else {
        throw new IllegalMigrationException(
            String.format("Destination is not up-to-date; path %s, call id %s",
                locations.getPath(), RPC.Server.getCallId()));
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalMigrationException(
          String.format("Cannot compare file to directory; path %s, call id %s",
              locations.getPath(), RPC.Server.getCallId()), e);
    }
  }

  /**
   * Get the location with a lease, else the dst if no lease is active
   * @param locations The remote locations as a pair
   * @return The location with a lease, else the dst if no lease is active
   * @throws IOException If an error occurs while retrieving file information or
   * there is more than once lease
   */
  private RemoteLocation getLeasedLocation(
      MigrationPair<RemoteLocation> locations) throws IOException {
    Map<RemoteLocation, LocatedBlocks> blockLocations = getBlockLocations(
        locations.asList());
    // If either source or destination blocks is null, it is not leased
    LocatedBlocks srcBlocks =
        blockLocations.getOrDefault(locations.getSrc(), new LocatedBlocks());
    boolean srcLeased = srcBlocks != null && srcBlocks.isUnderConstruction();
    LocatedBlocks dstBlocks =
        blockLocations.getOrDefault(locations.getDst(), new LocatedBlocks());
    boolean dstLeased = dstBlocks != null && dstBlocks.isUnderConstruction();
    if (srcLeased && dstLeased) {
      // The lease should never be open on the source and destination
      // simultaneously; if it does happen, fail the operation to bubble the
      // error to the client, which is preferable to allowing two different
      // writes to the source and destination (which could result in data loss
      // or corruption).
      throw new IllegalMigrationException(
          String.format("Both source and destination are leased; path %s,"
              + " call id %s", locations.getPath(), RPC.Server.getCallId()));
    } else if (srcLeased) {
      return locations.getSrc();
    } else if (dstLeased) {
      return locations.getDst();
    } else {
      // Since leases are used for writes, the absence of a lease should also
      // be treated as a write operation, therefore use destination location.
      LOG.debug(
          "Neither source or destination is leased, so using destination;"
          + " path {} call id {}", locations.getPath(), RPC.Server.getCallId());
      return getDestinationLocation(locations);
    }
  }

  /**
   * Invokes getFileInfo for multiple locations, bypassing migration logic.
   * Uses max batch size to avoid excessive fan-out.
   * @param locations The locations from which file status should be retrieved
   * @return A map of the locations and their corresponding file status
   * @throws IOException If an error occurs while retrieving file status
   */
  private Map<RemoteLocation, HdfsFileStatus> getFileInfo(
      Collection<RemoteLocation> locations) throws IOException {
    RemoteMethod method =
        new RemoteMethod("getFileInfo", new Class<?>[]{String.class},
            new RemoteParam());
    return invokeBatched(locations, method, HdfsFileStatus.class,
        batchSize, BatchHandling.CONTINUE);
  }

  /**
   * Invokes getBlockLocations for multiple locations, bypassing migration
   * logic.
   * @param locations The locations from which file status should be retrieved
   * @return A map of the locations and their corresponding block locations
   * @throws IOException If an error occurs while retrieving block locations
   */
  private Map<RemoteLocation, LocatedBlocks> getBlockLocations(
      Collection<RemoteLocation> locations) throws IOException {
    RemoteMethod method = new RemoteMethod("getBlockLocations",
        new Class<?>[]{String.class, long.class, long.class}, new RemoteParam(),
        0, Long.MAX_VALUE);
    return invokeBatched(locations, method, LocatedBlocks.class,
        batchSize, BatchHandling.CONTINUE);
  }

  /**
   * Enum for handling exceptions in batched invocations.
   */
  private enum BatchHandling {
    /**
     * Immediately return results if an exception occurs on any invocation
     */
    SHORT_CIRCUIT,
    /**
     * Throw an exception if an exception occurs on any invocation
     */
    THROW,
    /**
     * Continue processing if an exception occurs on a batch, only throwing an
     * exception if all calls fail (similar to RouterRpcClient#invokeConcurrent)
     */
    CONTINUE
  }

  /**
   * Invokes rpcClient methods for multiple locations in batches to control
   * fan-out, bypassing migration logic.
   * @param locations The locations to which the methods should be invoked
   * @param maxBatchSize The maximum number of methods to invoke at once
   *                     (0 for unlimited, 1 for sequential)
   * @param exceptionHandling The handling of exceptions in batched invocations
   * @return A map of the locations and the results of the methods
   * @throws IOException If all calls in a batch throw an exception; enabling
   *                     short-circuit logic in case of repeated failures
   */
  private <T> Map<RemoteLocation, T> invokeBatched(
      Collection<RemoteLocation> locations, RemoteMethod method, Class<T> clazz,
      int maxBatchSize, BatchHandling exceptionHandling)
      throws IOException {
    Map<RemoteLocation, T> results = new HashMap<>();
    IOException lastException = null;
    for (Iterator<RemoteLocation> iter = locations.iterator();
        iter.hasNext(); ) {
      List<RemoteLocation> batch = new ArrayList<>();
      while (iter.hasNext() && ((maxBatchSize <= 0) || (batch.size()
          < maxBatchSize))) {
        batch.add(iter.next());
      }
      List<RemoteResult<RemoteLocation, T>> batchResults =
          rpcClient.invokeConcurrent(batch, method, false, -1, clazz);
      for (RemoteResult<RemoteLocation, T> result : batchResults) {
        if (result.hasException()) {
          if (exceptionHandling == BatchHandling.THROW) {
            throw result.getException();
          }
          lastException = result.getException();
        } else if (result.hasResult()) {
          results.put(result.getLocation(), result.getResult());
        }
      }
      if (lastException != null) {
        if (exceptionHandling == BatchHandling.SHORT_CIRCUIT) {
          // Assign null values for unmapped keys
          for (RemoteLocation location : locations) {
            results.putIfAbsent(location, null);
          }
          return results;
        } else {
          LOG.warn("Potentially ignoring batched exception; call id {}",
              RPC.Server.getCallId(), lastException);
        }
      }
    }
    // Throw the last exception if present and no results were returned
    if (lastException != null && results.isEmpty()) {
      throw lastException;
    }
    return results;
  }

  /**
   * Reset the migration context, used for testing.
   */
  @VisibleForTesting
  void resetContext() {
    context.reset();
  }

  /**
   * A migration pair defines a source and destination of the same type,
   * typically RemoteLocation.
   */
  private static class MigrationPair<T> {
    private final T src;
    private final T dst;

    public MigrationPair(T src, T dst) {
      this.src = src;
      this.dst = dst;
    }

    public static <T> MigrationPair<T> of(T src, T dst) {
      return new MigrationPair<>(src, dst);
    }

    public T getSrc() {
      return src;
    }

    public T getDst() {
      return dst;
    }

    public List<T> asList() {
      return Arrays.asList(src, dst);
    }
    
    public String getPath() {
      if (src != null) {
        return src.toString();
      } else if (dst != null) {
        return dst.toString();
      } else {
        return null;
      }
    }
  }

  /**
   * Holds the current migration context, which is used to ensure operations are
   * handled consistently from beginning to end. This uses a ThreadLocal cache
   * with the RPC call id as a key to ensure that once a handler thread services
   * a new operation, the context is reset.
   * <p>
   * When set() is called, the specified MigrationBehavior and references to the
   * current MigratingMountPointInfo and PathLocation are saved to the context.
   * This is the expected flow for operations which specify a migration
   * behavior, allowing them to call set the migration context exactly once when
   * the migration behavior is set and ensuring that the context is used for
   * the entire operation.
   * <p>
   * If set() is not called, the default MigrationBehavior.UNKNOWN and
   * references to the current MigratingMountPointInfo and PathLocation are
   * saved to the context with the invocation of the first getter. This is the
   * expected flow for operations which do not specify a migration behavior,
   * defaulting to a value of MigrationBehavior.UNDEFINED and ensuring that
   * the context is used for the remainder of the operation.
   */
  private class MigrationContextCache {
    private final ThreadLocal<Pair<Integer, MigrationContextEntry>> cache =
        ThreadLocal.withInitial(() -> null);

    /**
     * Set the current operation's migration behavior and save the context.
     * @param migrationBehavior The migration behavior to be set
     * @param path The path for which the migration behavior is set
     * @return The migration context entry that was created
     * @throws IOException If an error occurs
     */
    public MigrationContextEntry set(MigrationBehavior migrationBehavior,
        String path) throws IOException {
      MigrationContextEntry
          value = new MigrationContextEntry(migrationBehavior, path);
      cache.set(Pair.of(RPC.Server.getCallId(), value));
      return value;
    }

    /**
     * Reset the current migration context, used for testing.
     */
    @VisibleForTesting
    public void reset() {
      cache.remove();
    }

    /**
     * Get the current operation's migration context. If the context is not
     * set for the current operation, this saves the migration context with
     * the default migration behavior of UNDEFINED. This never returns null.
     * @param path The path for which the migration behavior is set
     * @return The migration context for the current operation
     * @throws IOException If an error occurs
     */
    private MigrationContextEntry get(String path) throws IOException {
      Pair<Integer, MigrationContextEntry> pair = cache.get();
      // Reset the context if it is not set or was set for an old operation
      if (pair == null || pair.getKey() != RPC.Server.getCallId()) {
        return set(MigrationBehavior.UNDEFINED, path);
      } else {
        return pair.getValue();
      }
    }

    /**
     * Get the current operation's migration behavior. If the context is
     * not set for the current operation, this saves the migration context
     * and returns the default migration behavior of UNDEFINED.
     * @param path The path for which the migration behavior is set
     * @return The migration behavior, or UNDEFINED if not set
     */
    public MigrationBehavior getMigrationBehavior(String path)
        throws IOException {
      return get(path).getMigrationBehavior();
    }

    /**
     * Get the current operation's migrating mount point info. If the context is
     * not set for the current operation, this saves the migration context and
     * returns the current migrating mount point info.
     * @param path The path to which the migrating mount point info should apply
     * @return The migrating mount point info, or null if not migrating
     */ 
    public MigratingMountPointInfo getMigratingMountPointInfo(String path)
        throws IOException {
      return get(path).getMigratingMountPointInfo();
    }

    /**
     * Get the current operation's remote locations. This returns the default
     * remote locations from when the context was saved only if they match
     * the path, else null. If the context is not set for the current operation,
     * this saves the migration context and returns the current remote
     * locations.
     * @param path The path for which the remote locations should be retrieved
     * @return The corresponding PathLocation, or null if not set
     */ 
    public PathLocation getRemoteLocations(String path) throws IOException {
      PathLocation defaultLocation = get(path).getDefaultLocation();
      if (defaultLocation != null && defaultLocation.getSourcePath() != null
          && defaultLocation.getSourcePath().equals(path)) {
        return defaultLocation;
      } else {
        return null;
      }
    }

    /**
     * The inner context object, accessed via MigrationContextCache.
     * All fields are immutable except overrideLocations.
     */
    private class MigrationContextEntry {
      private final MigrationBehavior migrationBehavior;
      private final MigratingMountPointInfo migratingMountPointInfo;
      private final PathLocation defaultLocation;

      public MigrationContextEntry(MigrationBehavior migrationBehavior,
          String path) throws IOException {
        this.migrationBehavior = migrationBehavior;
        this.migratingMountPointInfo =
            (path == null || getMountPoint(path) == null) ? null
                : getMountPoint(path).getMigratingMountPointInfo();
        this.defaultLocation = path == null ? null
            : MigratingMountTableResolver.super.getDestinationForPath(path);
      }

      public MigrationBehavior getMigrationBehavior() {
        return migrationBehavior;
      }
      
      public MigratingMountPointInfo getMigratingMountPointInfo() {
        return migratingMountPointInfo;
      }
      
      public PathLocation getDefaultLocation() {
        return defaultLocation;
      }
    }
  }
}
