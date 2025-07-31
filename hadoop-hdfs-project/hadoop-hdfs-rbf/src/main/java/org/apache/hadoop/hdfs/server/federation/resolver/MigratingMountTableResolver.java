package org.apache.hadoop.hdfs.server.federation.resolver;

import java.util.LinkedList;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.server.federation.metrics.MigrationMetrics;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclStatus;
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
import static org.apache.hadoop.hdfs.server.federation.metrics.MigrationMetrics.CounterMetric.*;
import static org.apache.hadoop.hdfs.server.federation.metrics.MigrationMetrics.GaugeMetric.*;
import static org.apache.hadoop.hdfs.server.federation.metrics.MigrationMetrics.QuantileMetric.*;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;


/**
 * Handles mount point migration by determining the destination(s) for the given
 * path(s).
 */
public class MigratingMountTableResolver extends MountTableResolver {
  private static final Logger LOG =
      LoggerFactory.getLogger(MigratingMountTableResolver.class);
  private final Configuration conf;
  private RouterRpcServer rpcServer = null;
  private RouterRpcClient rpcClient = null;

  /**
   * A wrapper for the current migration context, used to ensure operations
   * are handled consistently from beginning to end.
   */
  private final MigrationContextCache context;

  /**
   * An instance of a helper class to assist with copying missing parents.
   */
  private final MissingPathHandler missingPathHandler;

  /**
   * The temporary staging location used to create missing directories.
   */
  private final String tempStagingSubdir;

  /**
   * The number of destination block locations that can be batched currently,
   * allowing to throttle latency vs fanout.
   */
  private final int batchSize;

  /**
   * The nameservices configured in the cluster.
   */
  private static final Collection<String> nameServices = new HashSet<>();

  /**
   * The migration metrics for the resolver.
   */
  private MigrationMetrics migrationMetrics;

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

    this.conf = conf;

    tempStagingSubdir = conf.get(DFS_ROUTER_MIGRATION_TEMP_STAGING_SUBDIR,
        DFS_ROUTER_MIGRATION_TEMP_STAGING_SUBDIR_DEFAULT);
    /*
     * Latency of ops during migration will increase, however a minimum batch
     * size of two should cause latency to not more than double without causing
     * excessive fan-out, except for create ops. A batch size of three allows
     * latency to not more than double for create ops, unless there are missing
     * parent directories.
     */
    batchSize = conf.getInt(DFS_ROUTER_MIGRATION_BATCH_SIZE, 2);
    LOG.debug("Using migration batch size {}", batchSize);

    missingPathHandler = new MissingPathHandler();

    nameServices.addAll(conf.getTrimmedStringCollection(DFS_NAMESERVICES));
    context = new MigrationContextCache();

    migrationMetrics = MigrationMetrics.create(conf);
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
   * Set the RPC client for the resolver. This is intended strictly for testing.
   * @param rpcClient The RPC client to be used by the resolver
   */
  @VisibleForTesting
  public void setRpcClient(RouterRpcClient rpcClient) {
    this.rpcClient = rpcClient;
  }

  /**
   * The defined behavior of the current operation during mount point migration.
   */
  public enum MigrationBehavior {
    /**
     * Use locations on both the source and destination for metadata-only
     * operations. Intended for list operations.
     */
    UNION,
    /**
     * Use locations on either the source or destination, whichever has the
     * latest mod time, or else the destination for directories or if mod times
     * are equal. Intended for reads.
     */
    LATEST,
    /**
     * Use locations on either the source or destination, whichever has an
     * active lease, or else the destination for directories or if if there is
     * no active lease. Intended for write data pipeline operations.
     * <p>
     * This is needed because in-flight writes to the source may call operations
     * (e.g. addBlock) that must be sent to the source even when mod times are
     * equal; however during migration, data pipeline operations (e.g. addBlock)
     * are generally sent to the destination. This can be replaced to LATEST if
     * all in-flight writes to the source are completed before files are copied
     * to the destination.
     */
    LEASED,
    /**
     * Use the destination location only. Intended for create, mkdirs, and
     * internal calls.
     * <p>
     * Since some directories may not yet be copied to the destination and this
     * does not otherwise check path presence or mod times, this checks for
     * directories that exist on the source but not the destination and copies
     * them.
     */
    DST_ONLY,
    /**
     * Use the source location only. Intended for internal calls.
     */
    SRC_ONLY,
    /**
     * Migration behavior is not defined; operations will fail unless set.
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
      String path) throws IOException {
    if (rpcClient == null || rpcServer == null) {
      throw new IllegalMigrationException(
          "MigratingMountTableResolver not initialized");
    }

    // If context is set, this method is internal to an existing migration op
    if (context.isSet()) {
      // Use the saved behavior and ignore the specified migration behavior
      LOG.debug("Migration behavior is already set to {} on {}; call id {}",
          context.get().getMigrationBehavior(), path, getUUID());
      return;
    }

    MigratingMountPointInfo migratingMountPointInfo =
        context.create(migrationBehavior, path).getMigratingMountPointInfo();
    Server.Call call = RPC.Server.getCurCall().get();

    if (migratingMountPointInfo != null && call == null) {
      // Migration is not supported if there is no associated RPC call
      throw new IllegalMigrationException("Migration only supported for RPC");
    } else if (migratingMountPointInfo != null) {
      // For migration via RPC, record the mount point migration and behavior
      LOG.info(
          "Using migration behavior {} for {} on {} ({}->{}); call id {}",
          migrationBehavior, call.getDetailedMetricsName(), path,
          migratingMountPointInfo.getSrcNs(),
          migratingMountPointInfo.getDstNs(), getUUID());
      // Add metrics alias to indicate this op is migrating
      call.addDetailedMetricsAlias("Migrating"
          + StringUtils.capitalize(call.getDetailedMetricsName()));

      // Only DST_ONLY ops may encounter missing directories, as they do not
      // check latest
      if (migrationBehavior == MigrationBehavior.DST_ONLY) {
        // Since the context is not already set, it is safe to create missing
        // paths here; otherwise assume missing paths are already created
        MigrationPairList<RemoteLocation> missingPaths =
            missingPathHandler.getMissingParentPaths(new Path(path));
        if (!missingPaths.isEmpty()) {
          missingPathHandler.copyMissingPathsFromSrc(missingPaths);
        }
      }
    } else if (call != null) {
      // If there is an associated RPC call, add a non-migrating metrics alias
      call.addDetailedMetricsAlias("NonMigrating" + StringUtils.capitalize(
          call.getDetailedMetricsName()));
    }
  }

  /**
   * Check if either of the mount tables is migrating and migration is enabled
   * @param newEntry optional new mount table entry.
   * @param oldEntry optional old mount table entry.
   * @return true if either of the mount tables is migrating
   */
  public static boolean isMigrating(FileSubclusterResolver resolver,
      @Nullable MountTable newEntry, @Nullable MountTable oldEntry) {
    MigratingMountPointInfo newMigrationInfo =
        newEntry == null ? null : newEntry.getMigratingMountPointInfo();
    MigratingMountPointInfo oldMigrationInfo =
        oldEntry == null ? null : oldEntry.getMigratingMountPointInfo();
    return resolver instanceof MigratingMountTableResolver &&
        (newMigrationInfo != null || oldMigrationInfo != null);
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
  public MountTable reconcileEntryWithMigration(@Nullable MountTable newEntry,
      @Nullable MountTable oldEntry) throws IllegalMigrationException {
    // The MigratingMountPointInfo that is being set; if not set, this
    // implies a migration is being removed.
    MigratingMountPointInfo newMigrationInfo =
        newEntry == null ? null : newEntry.getMigratingMountPointInfo();
    // The MigratingMountPointInfo that was already set; if not set, this
    // implies a migration is being added.
    MigratingMountPointInfo oldMigrationInfo =
        oldEntry == null ? null : oldEntry.getMigratingMountPointInfo();

    // Only reconcile if mount table is already migrating or is about to start
    // migrating; if neither, this is a no-op.
    if (newMigrationInfo != null || oldMigrationInfo != null) {
      Set<String> newNsIds = newEntry == null ? Collections.emptySet()
          : newEntry.getDestinations()
              .stream()
              .map(RemoteLocation::getNameserviceId)
              .collect(Collectors.toSet());

      // Determine the source and destination namespaces from the new migration
      // info if possible, else use the old migration info, else null
      String srcNsId = newMigrationInfo != null ?
          newMigrationInfo.getSrcNs() : oldMigrationInfo.getSrcNs();
      String dstNsId = newMigrationInfo != null ?
          newMigrationInfo.getDstNs() : oldMigrationInfo.getDstNs();
      String sourcePath = newEntry != null ? newEntry.getSourcePath()
          : oldEntry.getSourcePath();

      // The source and destination can never be the same
      if (srcNsId.equals(dstNsId)) {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table cannot have the same source and destination (%s->%s) for"
            + " %s", srcNsId, dstNsId, sourcePath));
      }

      // The mount table can only use namespaces that are migrating
      if (!Arrays.asList(srcNsId, dstNsId).containsAll(newNsIds)) {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table must use only namespaces used in migration (%s->%s), but"
            + " encountered %s for %s", srcNsId, dstNsId, newNsIds,
            sourcePath));
      }

      if (newMigrationInfo != null && oldMigrationInfo == null) {
        // Add migration info, since the mount point was not already migrating
        if (!newNsIds.contains(srcNsId)) {
          throw new IllegalMigrationException(String.format("Migrating mount"
              + " table must have the source namespace, but encountered %s for"
              + " %s", newNsIds, sourcePath));
        }
        if (newNsIds.size() == 1) {
          // If there is only one namespace, add the destination
          newNsIds.add(dstNsId);
        }
        LOG.info("Adding migration info ({} => {}->{}) for {}",
            srcNsId, srcNsId, dstNsId, sourcePath);
        migrationMetrics.incrGaugeMetric(GM_NUM_ACTIVE_MIGRATIONS, srcNsId,
            dstNsId);
      } else if (newMigrationInfo == null) {
        // Removing migration info, since the new mount point is not migrating
        if (newNsIds.size() > 1) {
          // If there are two namespaces, keep only the source
          newNsIds.remove(dstNsId);
        }
        String newNsId = newNsIds.isEmpty() ? null : newNsIds.iterator().next();
        LOG.info("Removing migration info ({}->{} => {}) for {}",
            srcNsId, dstNsId, newNsId, sourcePath);
        migrationMetrics.decrGaugeMetric(GM_NUM_ACTIVE_MIGRATIONS, srcNsId,
            dstNsId);
      } else {
        // Updating migration info, since the new mount point is migrating and
        // the old mount point was already migrating
        if (!newNsIds.contains(srcNsId)) {
          throw new IllegalMigrationException(String.format("Migrating mount"
              + " table must have the source namespace, but encountered %s for"
              + " %s", newNsIds, sourcePath));
        }
        if (newNsIds.size() == 1) {
          // If there is only one namespace, add the destination
          newNsIds.add(dstNsId);
        }
        // Ensure that any changes to the migration info are valid
        verifyMigrationUpdate(newMigrationInfo, oldMigrationInfo, sourcePath);

        // If there are changes to the migration info, update metrics
        if (!newMigrationInfo.equals(oldMigrationInfo)) {
          migrationMetrics.incrGaugeMetric(GM_NUM_ACTIVE_MIGRATIONS,
              newMigrationInfo.getSrcNs(), newMigrationInfo.getDstNs());
          migrationMetrics.decrGaugeMetric(GM_NUM_ACTIVE_MIGRATIONS,
              oldMigrationInfo.getSrcNs(), oldMigrationInfo.getDstNs());
        }
      }
      
      // Reconcile the entry locations to match the migration only if the mount
      // point is not being removed
      if (newEntry != null && newNsIds.isEmpty()) {
        throw new IllegalMigrationException(String.format("Migrating mount"
            + " table must have at least one destination, but encountered none"
            + " for %s", sourcePath));
      } else if (newEntry != null) {
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
  private void verifyMigrationUpdate(
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
  private void reconcileEntryLocations(MountTable newEntry,
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
    if (context.getOrCreate(MigrationBehavior.UNDEFINED, path)
        .getMigratingMountPointInfo() == null) {
      // There is no saved context if the mount point is not migrating or
      // if the migration behavior is not set.
      return super.getDestinationForPath(path);
    }
    try {
      MigrationPair<RemoteLocation> remoteLocations = getRemoteLocations(path);
      List<RemoteLocation> targetLocations;
      MigrationBehavior migrationBehavior = context.get().getMigrationBehavior();
      switch (migrationBehavior) {
        case SRC_ONLY:
          targetLocations =
              Collections.singletonList(remoteLocations.getSrc());
          break;
        case DST_ONLY:
          // A check whether the path exists on the source subcluster has
          // already been performed as part of the missing paths check;
          // see pathPresenceCache#getMissingParentDirectories.
          targetLocations =
              Collections.singletonList(remoteLocations.getDst());
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
              + " migration behavior; call id %s", getUUID()));
        default:
          // Cause new migration behaviors to fail until defined
          throw new UnsupportedOperationException(String.format(
              "Migration behavior %s is not defined; call id %s",
              migrationBehavior, getUUID()));
      }
      LOG.debug("Using {} migration destinations; call id {}",
          targetLocations.stream()
              .map(RemoteLocation::getNameserviceId)
              .collect(Collectors.joining(", ")), getUUID());
      incrOpCounter(targetLocations, remoteLocations);
      return new PathLocation(path, targetLocations);
    } finally {
      // Update metrics
      migrationMetrics.addQuantileMetric(QM_ROUTING_OPS,
          context.getSubOpCount());
      migrationMetrics.addQuantileMetric(QM_ROUTING_BATCHES,
          context.getSubOpBatchCount());
      context.resetOpCounts();
    }
  }

  /**
   * Increment the migration operation metrics counter for the src and/or dst
   * @param targetLocations The target locations for the operation
   * @param remoteLocations The source and destination remote locations
   */
  private void incrOpCounter(List<RemoteLocation> targetLocations,
      MigrationPair<RemoteLocation> remoteLocations) {
    String srcNs = remoteLocations.getSrc().getNameserviceId();
    String dstNs = remoteLocations.getDst().getNameserviceId();
    if (targetLocations.contains(remoteLocations.getSrc())) {
      migrationMetrics.incrCounterMetric(CM_NUM_SRC_OPS, srcNs, dstNs);
    }
    if (targetLocations.contains(remoteLocations.getDst())) {
      migrationMetrics.incrCounterMetric(CM_NUM_DST_OPS, srcNs, dstNs);
    }
    if (!targetLocations.isEmpty()) {
      migrationMetrics.incrCounterMetric(CM_NUM_OPS, srcNs, dstNs);
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
        context.get().getMigratingMountPointInfo();
    PathLocation pathLocation = context.get().getRemoteLocations(path);
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
          getUUID()));
    } else if (dstLocation == null) {
      throw new IllegalMigrationException(String.format("Migrating mount point"
          + " does not have a valid destination; path %s, call id %s", path,
          getUUID()));
    }
    return MigrationPair.of(srcLocation, dstLocation);
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
              locations.getPath(RemoteLocation::getSrc), getUUID()), e);
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
              + " call id %s", locations.getPath(RemoteLocation::getSrc),
              getUUID()));
    } else if (srcLeased) {
      return locations.getSrc();
    } else if (dstLeased) {
      return locations.getDst();
    } else {
      // Since leases are used for writes, the absence of a lease should also
      // be treated as a write operation, therefore use destination location.
      LOG.debug(
          "Neither source or destination is leased, so using destination;"
          + " path {} call id {}", locations.getPath(RemoteLocation::getSrc),
          getUUID());
      return locations.getDst();
    }
  }

  /**
   * A class to identify and copy paths that are present on the source but
   * missing on the destination.
   */
  private class MissingPathHandler {
    /**
     * Get the missing parent directories for the given path.
     * @param path The path for which to get the missing parent directories.
     * @return A list of remote locations that are missing on the destination
     * @throws IOException If an error occurs while checking the file status
     */
    private MigrationPairList<RemoteLocation> getMissingParentPaths(
        Path path) throws IOException {
      String sourcePath = context.get()
          .getRemoteLocations(path.toUri().getPath())
          .getSourcePath();
      MigratingMountPointInfo migratingMountPointInfo =
          context.get().getMigratingMountPointInfo();

      // Build a list of all possible paths
      MigrationPairList<RemoteLocation> locations = new MigrationPairList<>();
      for (Path p = path; p != null; p = p.getParent()) {
        String pathStr = p.toUri().getPath();
        if (pathStr.startsWith(sourcePath) && !pathStr.equals(sourcePath)) {
          locations.addFirst(getRemoteLocations(pathStr));
        } else {
          // Stop if the path is not valid
          break;
        }
      }

      MigrationPairList<RemoteLocation> missingLocations =
          MigrationPairList.empty();
      try {
        // There is nothing to do if there are no locations under migration
        if (!locations.isEmpty()) {
          // If the source path is missing, skip it and only consider parents;
          // else if the source path is a file, fail if it exists;
          // else if the source path is a dir, consider it missing with parents
          MigrationPair<RemoteLocation> pathLocations = locations.peekLast();
          Map<RemoteLocation, HdfsFileStatus> pathResults =
              getFileInfo(Collections.singletonList(pathLocations.getSrc()));
          if (pathResults.get(pathLocations.getSrc()) == null) {
            locations.removeLast();
          } else if (!pathResults.get(pathLocations.getSrc()).isDirectory()) {
            // Throw an error if the source file already exists
            // (no parent directories are missing)
            throw new IllegalMigrationException(String.format(
                "Path %s is present on the source, so cannot be "
                    + "recreated on dst; call id %s", pathLocations.getSrc(),
                getUUID()));
          }

          // Identify which source locations are present, short-circuiting on
          // the first batch with a missing path
          Map<RemoteLocation, HdfsFileStatus> existingSrcLocations =
              getFileInfoShort(locations.getSrcList());

          // Identify which destination locations are present, short-circuiting
          // on the first batch with a missing path
          Map<RemoteLocation, HdfsFileStatus> existingDstLocations =
              getFileInfoShort(locations.getDstList());

          // Identify locations present on the src and missing on the dst
          missingLocations = new MigrationPairList<>();
          for (MigrationPair<RemoteLocation> location : locations) {
            RemoteLocation srcLocation = location.getSrc();
            RemoteLocation dstLocation = location.getDst();

            if (existingSrcLocations.get(srcLocation) != null
                && existingDstLocations.get(dstLocation) == null) {
              missingLocations.add(location);
            }
          }
        }
        return missingLocations;
      } finally {
        // Update metrics
        migrationMetrics.addQuantileMetric(QM_MISSING_PARENT_DEPTH,
            missingLocations.size());
        if (!missingLocations.isEmpty()) {
          migrationMetrics.incrCounterMetric(CM_MISSING_PARENT_NUM_OPS,
              migratingMountPointInfo.getSrcNs(),
              migratingMountPointInfo.getDstNs());
        }
        migrationMetrics.addQuantileMetric(QM_MISSING_PARENT_DETECTION_OPS,
            context.getSubOpCount());
        migrationMetrics.addQuantileMetric(QM_MISSING_PARENT_DETECTION_BATCHES,
            context.getSubOpBatchCount());
        context.resetOpCounts();
      }
    }

    /**
     * Copy all missing directories from the source to the destination.
     * @param missingPaths The list of missing paths to copy
     * @throws IOException If an error occurs while copying the directories
     */
    private void copyMissingPathsFromSrc(
        MigrationPairList<RemoteLocation> missingPaths) throws IOException {
      try {
        // The original path is the last path in the list of missing paths
        String path = missingPaths.getLast().getPath(RemoteLocation::getSrc);
        // The mount point root is saved as the path location's source path
        String mpRoot = context.get().getRemoteLocations(path).getSourcePath();
        // Use the destination location in the prefix for the missing paths
        Path prefix = new Path(getMountPointTempPrefix(new Path(mpRoot)),
            UUID.randomUUID().toString());
        Map<RemoteLocation, AclStatus> aclStatusMap =
            getAclStatuses(missingPaths.getSrcList());
        // Copy all missing directories to the destination
        for (RemoteLocation location : missingPaths.getSrcList()) {
          copyDirectory(prefix, location.getSrc(), aclStatusMap.get(location));
        }
        // Commit all missing directories to the final location on the
        // destination, starting with the highest missing path
        commitDirectories(prefix, missingPaths);
        cleanupTmpDirectories(prefix);
      } finally {
        // Update metrics
        migrationMetrics.addQuantileMetric(QM_MISSING_PARENT_CREATION_OPS,
            context.getSubOpCount());
        migrationMetrics.addQuantileMetric(QM_MISSING_PARENT_CREATION_BATCHES,
            context.getSubOpBatchCount());
        context.resetOpCounts();
      }
    }

    /**
     * Copy a directory from source to destination, including all metadata.
     * @param prefix The prefix for the temporary directories
     * @param srcPath The source path to copy
     * @param aclStatus The ACL status of the source path
     * @throws IOException If an error occurs while copying the directory
     */
    private void copyDirectory(Path prefix, String srcPath, AclStatus aclStatus)
        throws IOException {
      String dstPath = getTmpPath(prefix, srcPath);
      LOG.info("Copying {} to {}; call id {}", srcPath, dstPath, getUUID());
      // Missing parents should be created in the tmp directory to allow missing
      // dirs to be created at any level, not just root. This requires directory
      // creation to be in order from parent to child.
      callNamenode(MigrationBehavior.DST_ONLY,
          () -> rpcServer.mkdirs(dstPath, aclStatus.getPermission(), true));
      callNamenode(MigrationBehavior.DST_ONLY,
          () -> rpcServer.setOwner(dstPath, aclStatus.getOwner(),
              aclStatus.getGroup()));
      callNamenode(MigrationBehavior.DST_ONLY,
          () -> rpcServer.setAcl(dstPath, aclStatus.getEntries()));
      // setAcl does not set the sticky bit, so we need to do it separately
      if (aclStatus.isStickyBit()) {
        callNamenode(MigrationBehavior.DST_ONLY,
            () -> rpcServer.setPermission(dstPath, aclStatus.getPermission()));
      }
      // Copying XAttrs is slow, but in most cases there should be few or none
      for (XAttr xAttr : callNamenode(MigrationBehavior.SRC_ONLY,
          () -> rpcServer.listXAttrs(srcPath))) {
        callNamenode(MigrationBehavior.DST_ONLY,
            () -> rpcServer.setXAttr(dstPath, xAttr,
                EnumSet.allOf(XAttrSetFlag.class)));
      }
      // Do not copy quotas, as they are not copied by DistCp
      // Do not copy times, as setTimes only supports files
    }
    
    /**
     * Commit the directories to the destination, including all metadata.
     * @param prefix The prefix for the temporary directories
     * @param missingPaths The list of missing paths to commit
     * @throws IOException If an error occurs while committing the directories
     */
    private void commitDirectories(Path prefix,
        MigrationPairList<RemoteLocation> missingPaths) throws IOException {
      // Rename the directories to the final location
      for (RemoteLocation dstLocation : missingPaths.getDstList()) {
        String dstPath = dstLocation.getSrc();
        String srcPath = getTmpPath(prefix, dstPath);
        LOG.info("Renaming {} to {}; call id {}", srcPath, dstPath, getUUID());
        try {
          callNamenode(MigrationBehavior.DST_ONLY,
              () -> rpcServer.rename2(srcPath, dstPath, Options.Rename.NONE));
          // Once the rename succeeds, short-circuit the rename of all children
          break;
        } catch (FileAlreadyExistsException e) {
          // If the rename fails because the directory already exists, then it
          // may have been copied by distcp; try again for each child
          LOG.info("Rename not needed for {}; call id {}", dstPath, getUUID());
        }
      }
    }

    /**
     * Cleanup the temporary directories created for the migration.
     * @param prefix The prefix for the temporary directories
     * @throws IOException If an error occurs while cleaning up the directories
     */
    private void cleanupTmpDirectories(Path prefix) throws IOException {
      // Remove the temporary directories created for the migration
      callNamenode(MigrationBehavior.DST_ONLY,
          () -> rpcServer.delete(prefix.toUri().getPath(), true));
    }

    /**
     * Get the temporary path for the given path, used to atomically copy
     * directories.
     * @param prefix The op-specific prefix for temporary directories
     * @param path The path to get the temporary path for
     * @return The temporary path for the given path
     */
    private String getTmpPath(Path prefix, String path) {
      // This uses filename utils to remove the leading slash
      return new Path(prefix, new Path(FilenameUtils.getPath(path),
          FilenameUtils.getName(path))).toUri().getPath();
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
    return invokeBatched(locations, method, HdfsFileStatus.class, batchSize,
        false);
  }

  /**
   * Invokes getFileInfo for multiple locations, bypassing router logic.
   * This method short-circuits on exceptions or null results to avoid
   * unnecessary calls.
   * @param locations The locations from which file status should be retrieved
   * @return A map of the locations and their corresponding file status
   * @throws IOException If an error occurs while retrieving file status
   */
  private Map<RemoteLocation, HdfsFileStatus> getFileInfoShort(
      Collection<RemoteLocation> locations) throws IOException {
    RemoteMethod method =
        new RemoteMethod("getFileInfo", new Class<?>[]{String.class},
            new RemoteParam());
    return invokeBatched(locations, method, HdfsFileStatus.class, batchSize,
        true);
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
    return invokeBatched(locations, method, LocatedBlocks.class, batchSize,
        false);
  }

  /**
   * Invokes getAclStatus for multiple locations, bypassing router logic.
   * Uses max batch size to avoid excessive fan-out.
   * @param locations The locations from which ACL status should be retrieved
   * @return A map of the locations and their corresponding AclStatus
   * @throws IOException If an error occurs while retrieving AclStatus
   */
  private Map<RemoteLocation, AclStatus> getAclStatuses(
      Collection<RemoteLocation> locations) throws IOException {
    RemoteMethod method = new RemoteMethod(
        "getAclStatus", new Class<?>[]{String.class}, new RemoteParam());
    return invokeBatched(locations, method, AclStatus.class, batchSize, false);
  }

  /**
   * Invokes rpcClient methods for multiple locations in batches to control
   * fan-out, bypassing migration logic.
   * @param locations The locations to which the methods should be invoked
   * @param maxBatchSize The maximum number of methods to invoke at once
   *                     (0 for unlimited, 1 for sequential)
   * @param doShortCircuit True to short-circuit if any result is null
   * @return A map of the locations and the results of the methods
   * @throws IOException If all calls in a batch throw an exception, enabling
   *                     short-circuit logic in case of repeated failures
   */
  private <T> Map<RemoteLocation, T> invokeBatched(
      Collection<RemoteLocation> locations, RemoteMethod method, Class<T> clazz,
      int maxBatchSize, boolean doShortCircuit) throws IOException {
    Map<RemoteLocation, T> results = new HashMap<>();
    IOException lastException = null;
    for (Iterator<RemoteLocation> iter = locations.iterator();
        iter.hasNext(); ) {
      List<RemoteLocation> batch = new ArrayList<>();
      while (iter.hasNext() && ((maxBatchSize <= 0) || (batch.size()
          < maxBatchSize))) {
        batch.add(iter.next());
        context.incrSubOpCount();
      }
      context.incrSubOpBatchCount();
      List<RemoteResult<RemoteLocation, T>> batchResults =
          rpcClient.invokeConcurrent(batch, method, false, -1, clazz);
      // Tracks whether a short-circuit is in progress
      boolean isShortCircuiting = false;
      for (RemoteResult<RemoteLocation, T> result : batchResults) {
        // Run validation function if present
        if (result.hasException()) {
          lastException = result.getException();
        } else if (result.hasResult()) {
          results.put(result.getLocation(), result.getResult());
          // If any result is null, short-circuit remaining batches
          if (doShortCircuit && result.getResult() == null) {
            isShortCircuiting = true;
          }
        }
      }
      if (isShortCircuiting) {
        LOG.debug("Short-circuiting batch due to no non-null responses; "
            + " call id {}", getUUID());
        // Assign null values for unmapped keys
        for (RemoteLocation location : locations) {
          results.putIfAbsent(location, null);
        }
        break;
      }
    }
    // Throw the last exception if present and no results were returned
    if (lastException != null && results.isEmpty()) {
      throw lastException;
    }
    return results;
  }

  @FunctionalInterface
  private interface RemoteMethodSupplier<T> {
    T get() throws IOException;
  }

  /**
   * Call the namenode directly, bypassing migration logic.
   * @param overrideBehavior The behavior specifying the target namenode
   * @param supplier A supplier of the method to be invoked
   * @return The result of the supplier
   * @throws IOException If an error occurs while invoking the supplier
   */
  private <T> T callNamenode(MigrationBehavior overrideBehavior,
      RemoteMethodSupplier<T> supplier) throws IOException {
    context.incrSubOpCount();
    context.incrSubOpBatchCount();
    return context.overrideBehavior(overrideBehavior, supplier);
  }

  @FunctionalInterface
  private interface RemoteMethodRunnable {
    void run() throws IOException;
  }

  /**
   * Call the namenode directly, bypassing migration logic.
   * @param overrideBehavior The behavior specifying the target namenode
   * @param runnable A runnable supplying the void method to be invoked
   * @throws IOException If an error occurs while invoking the runnable
   */
  private void callNamenode(MigrationBehavior overrideBehavior,
      RemoteMethodRunnable runnable) throws IOException {
    callNamenode(overrideBehavior, () -> {
      // Metrics are tracked through overloaded method
      runnable.run();
      return null; // void return type
    });
  }

  /**
   * Reset the migration context, used for testing.
   */
  @VisibleForTesting
  void resetContext() {
    context.reset();
  }

  /**
   * Get the temporary prefix for the mount point, used for migration.
   * @param sourcePath The source path for the mount point
   * @return The temporary prefix for the mount point
   */
  public Path getMountPointTempPrefix(Path sourcePath) {
    return new Path(sourcePath, tempStagingSubdir);
  }

  /**
   * Build a UUID from the current call. At present, this builds a UUID from:
   *  - The call timestamp, ensuring that sequential calls always have a UUID
   *  - The call id, ensuring that calls with the same timestamp have a UUID
   * @return A UUID based on the current RPC call, or null if there is no
   *         associated RPC call
   */
  @VisibleForTesting
  UUID getUUID() {
    if (RPC.Server.getCurCall() == null
        || RPC.Server.getCurCall().get() == null) {
      return null;
    } else {
      return new UUID(RPC.Server.getCallId(),
          RPC.Server.getCurCall().get().getTimestampNanos());
    }
  }

  @VisibleForTesting
  public void resetMigrationMetrics() {
    migrationMetrics = MigrationMetrics.create(conf);
  }

  @VisibleForTesting
  public MigrationMetrics getMigrationMetrics() {
    return migrationMetrics;
  }

  private static class MigrationPairList<T>
      extends LinkedList<MigrationPair<T>> {
    private static final MigrationPairList<?> EMPTY_LIST =
        new MigrationPairList<>(Collections.emptyList());
    
    @SuppressWarnings("unchecked")
    public static <M> MigrationPairList<M> empty() {
      return (MigrationPairList<M>) EMPTY_LIST;
    }
    
    public MigrationPairList() {
      super();
    }

    public MigrationPairList(List<MigrationPair<T>> pairs) {
      super(pairs);
    }
    
    public List<T> getSrcList() {
      return this.stream()
          .map(MigrationPair::getSrc)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    public List<T> getDstList() {
      return this.stream()
          .map(MigrationPair::getDst)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
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

    /**
     * Get the source and destination as a list.
     * @return A list containing the source and destination
     */
    public List<T> asList() {
      return Arrays.asList(src, dst);
    }

    /**
     * Get the path for the source or destination, whichever is non-null,
     * using the provided function to convert the object to a path.
     * @param toPath Function to convert the object to a path
     * @return The path for the source or destination, or null if both are null
     */
    public String getPath(Function<T, String> toPath) {
      if (src != null) {
        return toPath.apply(src);
      } else if (dst != null) {
        return toPath.apply(dst);
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
    private final ThreadLocal<Pair<UUID, MigrationContextEntry>> cache =
        ThreadLocal.withInitial(() -> null);

    /**
     * Check if the current operation's migration context is set.
     * @return True if the context is set, false otherwise
     */
    public boolean isSet() {
      UUID key = getUUID();
      if (key == null) {
        // If there is no UUID, the context cannot be set
        return false;
      }
      Pair<UUID, MigrationContextEntry> pair = cache.get();
      return pair != null && pair.getKey().equals(key);
    }

    /**
     * Create the migration context for the current operation.
     * @param migrationBehavior The migration behavior to be set
     * @param path The path for which the migration behavior is set
     * @throws IllegalMigrationException If an error occurs
     */
    public MigrationContextEntry create(MigrationBehavior migrationBehavior,
        String path) throws IOException {
      UUID key = getUUID();
      if (key == null) {
        return new MigrationContextEntry(MigrationBehavior.UNDEFINED, path);
      } else if (isSet()) {
        throw new IllegalMigrationException(
            "Migration context cannot be overwritten");
      } else {
        MigrationContextEntry entry =
            new MigrationContextEntry(migrationBehavior, path);
        cache.set(Pair.of(key, entry));
        return entry;
      }
    }

    /**
     * Get the current operation's migration context, or create the migration
     * context if it does not already exist. This never returns null.
     * @param migrationBehavior The migration behavior to use for creation
     * @param path The path for which the migration behavior is set
     * @return The migration context for the current operation
     * @throws IOException If an error occurs
     */
    public MigrationContextEntry getOrCreate(
        MigrationBehavior migrationBehavior, String path) throws IOException {
      UUID key = getUUID();
      if (key == null) {
        return new MigrationContextEntry(MigrationBehavior.UNDEFINED, path);
      } else if (isSet()) {
        return get();
      } else {
        MigrationContextEntry entry =
            new MigrationContextEntry(migrationBehavior, path);
        cache.set(Pair.of(key, entry));
        return entry;
      }
    }

    /**
     * Get the current operation's migration context. If the context is not
     * set for the current operation, this throws an exception.
     * @return The migration context for the current operation
     * @throws IllegalMigrationException If the migration context is not set
     */
    public MigrationContextEntry get() throws IllegalMigrationException {
      Pair<UUID, MigrationContextEntry> pair = cache.get();
      // Reset the context if it is not set or was set for an old operation
      if (pair == null || !pair.getKey().equals(getUUID())) {
        throw new IllegalMigrationException(
            String.format("Migration context is not set; call id %s",
                getUUID()));
      } else {
        return pair.getValue();
      }
    }

    /**
     * Reset the current migration context, used for testing.
     */
    @VisibleForTesting
    public void reset() {
      cache.remove();
    }

    /**
     * Override the migration behavior while running the provided supplier.
     * @param overrideBehavior The migration behavior to use
     * @param supplier The supplier to run with the overridden behavior
     * @return The result of the supplier
     * @param <T> The type of the result returned by the supplier
     * @throws IOException If an error occurs while running the supplier
     */
    public <T> T overrideBehavior(MigrationBehavior overrideBehavior,
        RemoteMethodSupplier<T> supplier) throws IOException {
      MigrationContextEntry contextEntry = get();
      try {
        contextEntry.setOverrideBehavior(overrideBehavior);
        return supplier.get();
      } finally {
        contextEntry.resetOverrideBehavior();
      }
    }

    /**
     * Increment the number of sub ops for the current migration.
     */
    public void incrSubOpCount() throws IllegalMigrationException {
      MigrationContextEntry contextEntry = get();
      if (contextEntry != null) {
        contextEntry.subOpCount++;
      }
    }

    /**
     * Get the number of sub ops for the current migration.
     * @return The number of sub ops for the current migration
     */
    public int getSubOpCount() throws IllegalMigrationException {
      MigrationContextEntry contextEntry = get();
      if (contextEntry != null) {
        return contextEntry.subOpCount;
      }
      return 0;
    }

    /**
     * Increment the number of sub op batches for the current migration.
     */
    public void incrSubOpBatchCount() throws IllegalMigrationException {
      MigrationContextEntry contextEntry = get();
      if (contextEntry != null) {
        contextEntry.subOpBatchCount++;
      }
    }

    /**
     * Get the number of sub op batches for the current migration.
     * @return The number of sub op batches for the current migration
     */
    public int getSubOpBatchCount() throws IllegalMigrationException {
      MigrationContextEntry contextEntry = get();
      if (contextEntry != null) {
        return contextEntry.subOpBatchCount;
      }
      return 0;
    }

    /**
     * Reset the sub op counts for the current migration.
     */
    public void resetOpCounts() throws IllegalMigrationException {
      MigrationContextEntry contextEntry = get();
      if (contextEntry != null) {
        contextEntry.subOpCount = 0;
        contextEntry.subOpBatchCount = 0;
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
      private MigrationBehavior overrideBehavior;

      private int subOpCount = 0;
      private int subOpBatchCount = 0;

      public MigrationContextEntry(MigrationBehavior migrationBehavior,
          String path) throws IOException {
        this.migrationBehavior = migrationBehavior;
        this.migratingMountPointInfo =
            (path == null || getMountPoint(path) == null) ? null
                : getMountPoint(path).getMigratingMountPointInfo();
        this.defaultLocation = path == null ? null
            : MigratingMountTableResolver.super.getDestinationForPath(path);
        // By default, the override behavior is undefined
        this.overrideBehavior = MigrationBehavior.UNDEFINED;
      }

      public MigrationBehavior getMigrationBehavior() {
        if (overrideBehavior != MigrationBehavior.UNDEFINED) {
          return overrideBehavior;
        }
        return migrationBehavior;
      }
      
      public MigratingMountPointInfo getMigratingMountPointInfo() {
        return migratingMountPointInfo;
      }
      
      public PathLocation getRemoteLocations(String path) throws IOException {
        if (defaultLocation != null && defaultLocation.getSourcePath() != null
          && defaultLocation.getDefaultLocation().getSrc().equals(path)) {
          return defaultLocation;
        } else {
          LOG.warn("Path {} is not in the migration context; call id {}",
              path, getUUID());
          return MigratingMountTableResolver.super.getDestinationForPath(path);
        }
      }
      
      private void setOverrideBehavior(MigrationBehavior overrideBehavior) {
        this.overrideBehavior = overrideBehavior;
      }
      
      private void resetOverrideBehavior() {
        this.overrideBehavior = MigrationBehavior.UNDEFINED;
      }
    }
  }
}
