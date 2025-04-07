package org.apache.hadoop.hdfs.server.federation.resolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;


/**
 * Handles mount point migration by determining the destination(s) for the given
 * path(s).
 */
public class MigratingMountTableResolver extends MountTableResolver {
  private static final Logger LOG =
      LoggerFactory.getLogger(MigratingMountTableResolver.class);
  /**
   * The nameservices configured in the cluster.
   */
  private static final Collection<String> nameServices = new HashSet<>();

  public MigratingMountTableResolver(Configuration conf, Router routerService) {
    super(conf, routerService);

    nameServices.addAll(conf.getTrimmedStringCollection(DFS_NAMESERVICES));
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
}
