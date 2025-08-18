package org.apache.hadoop.hdfs.server.federation.metrics;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.MigratingMountTableResolver.MigrationPair;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;


@Metrics(name="MigrationMetrics", about="Migration metrics", context="dfs")
public class MigrationMetrics implements MetricsSource {
  private final MetricsRegistry registry;
  private final int[] quantileIntervals;

  /**
   * Create a new instance of MigrationMetrics and register it with the
   * MetricsSystem. If an instance already exists, it will be unregistered
   * and replaced with the new instance.
   * @return the MigrationMetrics instance
   */
  public static MigrationMetrics create(Configuration conf) {
    int[] quantileIntervals;
    if (conf.getBoolean(RBFConfigKeys.MIGRATION_METRICS_QUANTILE_ENABLE,
        false)) {
      quantileIntervals =
          conf.getInts(RBFConfigKeys.MIGRATION_METRICS_PERCENTILES_INTERVALS);
    } else {
      quantileIntervals = new int[0];
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(MigrationMetrics.class.getSimpleName());
    return ms.register(MigrationMetrics.class.getSimpleName(),
        "Migration metrics", new MigrationMetrics(quantileIntervals));
  }

  /**
   * Get the name of the MigrationMetrics, useful for locating metrics in the
   * MetricsSystem. This must match the name in the class annotation.
   * @return the name of the MigrationMetrics
   */
  public static String getName() {
    return MigrationMetrics.class.getSimpleName();
  }

  /**
   * Private constructor to initialize the MigrationMetrics instance.
   * Resets the registry and initializes gauge and counter metrics.
   */
  private MigrationMetrics(int[] quantileIntervals) {
    this.quantileIntervals = quantileIntervals;
    registry = new MetricsRegistry("router");
    // Initialize gauge metrics
    for (GaugeMetric metric : GaugeMetric.values()) {
      gaugeMetricsMap.put(metric.metricName,
          registry.newGauge(metric.metricName, metric.metricDesc, 0L));
    }
    // Initialize counter metrics
    for (CounterMetric metric : CounterMetric.values()) {
      counterMetricsMap.put(metric.metricName,
          registry.newCounter(metric.metricName, metric.metricDesc, 0L));
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // Synchronized block allows multiple specific metrics to be set atomically
    synchronized (this) {
      registry.snapshot(collector.addRecord(registry.info()), all);
    }
  }

  @VisibleForTesting
  public MetricsRegistry getRegistry() {
    return registry;
  }

  /**
   * Gauge Metrics
   */
  public enum GaugeMetric {
    GM_NUM_ACTIVE_MIGRATIONS("MigrationNumActiveMigrations",
        "Current number of migrations in progress");

    private final String metricName;
    private final String metricDesc;

    GaugeMetric(String metricName, String metricDesc) {
      this.metricName = metricName;
      this.metricDesc = metricDesc;
    }

    @Override
    public String toString() {
      return metricName;
    }
  }

  private final Map<String, MutableGaugeLong> gaugeMetricsMap =
      new ConcurrentHashMap<>();

  /**
   * Set a generic gauge metric to the provided value.
   * @param metric the gauge metric to set
   * @param val the value to set the gauge metric to
   */
  public void setGenericGaugeMetric(GaugeMetric metric, long value) {
    gaugeMetricsMap.computeIfAbsent(metric.metricName,
        k -> registry.newGauge(metric.metricName, metric.metricDesc, value))
        .set(value);
  }

  /**
   * Set a specific gauge metric for a given source and destination to the
   * provided value. This does not update the generic metric.
   * @param metric the gauge metric to set
   * @param srcNs the source namespace
   * @param dstNs the destination namespace
   * @param val the value to set the specific gauge metric to
   */
  public void setSpecificGaugeMetric(GaugeMetric metric, String srcNs,
      String dstNs, long value) {
    String specificName =
        buildSpecificMetricName(metric.metricName, srcNs, dstNs);
    gaugeMetricsMap.computeIfAbsent(specificName,
        k -> registry.newGauge(specificName,
            buildSpecificMetricDesc(metric.metricName, srcNs, dstNs), value))
        .set(value);
  }

  /**
   * Set gauge metrics for the generic and all specific metrics provided
   * by the metricsMap atomically.
   * @param metric the gauge metric to set
   * @param metricsMap a map of migration src-dst pairs to their values
   */
  public void setGaugeMetricsMap(GaugeMetric metric,
      Map<MigrationPair<String>, Long> metricsMap) {
    // Synchronized block ensures all specific gauge metrics are set atomically
    synchronized (this) {
      // Reset all gauge metrics before setting new values
      for (MutableGaugeLong gauge : gaugeMetricsMap.values()) {
        gauge.set(0L);
      }

      // Set new specific metrics and calculate total value
      long totalValue = 0;
      for (Map.Entry<MigrationPair<String>, Long> entry :
          metricsMap.entrySet()) {
        // Set the specific metric
        setSpecificGaugeMetric(metric, entry.getKey().getSrc(),
            entry.getKey().getDst(), entry.getValue());

        totalValue += entry.getValue();
      }

      // Set total value for generic metric
      setGenericGaugeMetric(metric, totalValue);
    }
  }

  /**
   * Counter Metrics
   */
  public enum CounterMetric {
    CM_MISSING_PARENT_NUM_OPS("MigrationMissingParentNumOps",
        "Number of migration ops which have missing parent dirs"),
    CM_NUM_OPS("MigrationNumOps",
        "Number of migration ops issued to migrating mount points"),
    CM_NUM_SRC_OPS("MigrationSrcNumOps",
        "Number of ops issued to the src of a migrating mount point"),
    CM_NUM_DST_OPS("MigrationDstNumOps",
        "Number of ops issued to the dst of a migrating mount point");

    private final String metricName;
    private final String metricDesc;

    CounterMetric(String metricName, String metricDesc) {
      this.metricName = metricName;
      this.metricDesc = metricDesc;
    }

    @Override
    public String toString() {
      return metricName;
    }
  }

  private final Map<String, MutableCounterLong> counterMetricsMap =
      new ConcurrentHashMap<>();

  /**
   * Increment a counter metric by 1.
   * @param metric the counter metric to update
   * @param srcNs the source namespace, can be null
   * @param dstNs the destination namespace, can be null
   */
  public void incrCounterMetric(CounterMetric metric, String srcNs,
      String dstNs) {
    counterMetricsMap.computeIfAbsent(metric.metricName,
        k -> registry.newCounter(metric.metricName, metric.metricDesc, 0L))
        .incr();
    if (srcNs != null && dstNs != null) {
      String specificName =
          buildSpecificMetricName(metric.metricName, srcNs, dstNs);
      counterMetricsMap.computeIfAbsent(specificName,
          k -> registry.newCounter(specificName,
              buildSpecificMetricDesc(metric.metricName, srcNs, dstNs), 0L))
          .incr();
    }
  }

  /**
   * Quantile Metrics
   */
  public enum QuantileMetric {
    QM_ROUTING_OPS("MigrationRouting",
        "Internal ops issued to route migration ops", "Ops", "Num"),
    QM_ROUTING_BATCHES("MigrationRoutingBatching",
        "Batching of internal ops issued to route migration ops",
        "Batches", "Num"),
    QM_MISSING_PARENT_DETECTION_OPS("MigrationMissingParentDetection",
        "Internal ops issued to detect missing parent dirs",
        "Ops", "Num"),
    QM_MISSING_PARENT_DETECTION_BATCHES(
        "MigrationMissingParentDetectionBatching",
        "Batching of internal ops issued to detect missing parent dirs",
        "Batches", "Num"),
    QM_MISSING_PARENT_CREATION_OPS("MigrationMissingParentCreation",
        "Internal ops issued to create missing parent dirs",
        "Ops", "Num"),
    QM_MISSING_PARENT_CREATION_BATCHES(
        "MigrationMissingParentCreationBatching",
        "Batching of internal ops issued to create missing parent dirs",
        "Batches", "Num"),
    QM_MISSING_PARENT_DEPTH("MigrationMissingParentDepth",
        "Parent dirs missing during migration ops", "Dirs", "Num");

    private final String metricName;
    private final String metricDesc;
    private final String sampleName;
    private final String valueName;

    QuantileMetric(String metricName, String metricDesc, String sampleName,
        String valueName) {
      this.metricName = metricName;
      this.metricDesc = metricDesc;
      this.sampleName = sampleName;
      this.valueName = valueName;
    }

    @Override
    public String toString() {
      return metricName;
    }
  }

  private final Map<String, MutableQuantiles> quantileMetricsMap =
      new ConcurrentHashMap<>();

  /**
   * Add a quantile metric with a value.
   * @param metric the quantile metric to add
   * @param value the value to add to the quantile metric
   */
  public void addQuantileMetric(QuantileMetric metric, long value) {
    addQuantileMetric(metric, value, null, null);
  }

  /**
   * Add a quantile metric with a value, specifying source and destination
   * @param metric the quantile metric to add
   * @param value the value to add to the quantile metric
   * @param srcNs the source namespace, can be null
   * @param dstNs the destination namespace, can be null
   */
  public void addQuantileMetric(QuantileMetric metric, long value,
      String srcNs, String dstNs) {
    for (int interval : quantileIntervals) {
      String metricName = metric.metricName + interval + 's';
      quantileMetricsMap.computeIfAbsent(metricName,
          k -> registry.newQuantiles(metricName, metric.metricDesc,
              metric.sampleName, metric.valueName, interval)).add(value);
      if (srcNs != null && dstNs != null) {
        String specificName = buildSpecificMetricName(metricName, srcNs, dstNs);
        quantileMetricsMap.computeIfAbsent(specificName,
            k -> registry.newQuantiles(specificName,
                buildSpecificMetricDesc(metricName, srcNs, dstNs),
                metric.sampleName, metric.valueName, interval)).add(value);
      }
    }
  }

  /**
   * Get the median value for a given quantile metric. This directly exposes the
   * 50th percentile value from the quantile metrics objects to avoid timing
   * issues related to the collection interval.
   * @param metric the quantile metric for which to retrieve the median
   * @return the median value for the specified quantile metric
   * @throws IOException if the metric is not enabled or not found
   */
  @VisibleForTesting
  public long getQuantileMedian(QuantileMetric metric) throws IOException {
    if (quantileIntervals.length == 0) {
      throw new IOException("Quantile metrics are not enabled.");
    }
    int interval = quantileIntervals[0];
    MutableQuantiles quantile =
        quantileMetricsMap.get(metric.metricName + interval + 's');
    if (quantile == null) {
      // If the quantile metric is not found, no values have been added
      return 0L;
    }
    return quantile.getEstimator()
        .snapshot()
        .get(MutableQuantiles.QUANTILES[0]);
  }

  /**
   * Build a specific metric name based on the provided metric name and the
   * source and destination namespaces.
   * @param metricName the base metric name
   * @param srcNs the source namespace
   * @param dstNs the destination namespace
   * @return the specific metric name
   */
  private String buildSpecificMetricName(String metricName, String srcNs,
      String dstNs) {
    return metricName + "." + srcNs + "-" + dstNs;
  }

  /**
   * Build a specific metric description based on the provided metric and the
   * source and destination namespaces.
   * @param metricDesc the base metric description
   * @param srcNs the source namespace
   * @param dstNs the destination namespace
   * @return the specific metric description
   */
  private String buildSpecificMetricDesc(String metricDesc, String srcNs,
      String dstNs) {
    return metricDesc + " on " + srcNs + "-" + dstNs;
  }
}
