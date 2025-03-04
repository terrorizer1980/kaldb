package com.slack.kaldb.histogram;

import javax.annotation.Nonnull;

/**
 * A HistogramBucket represents one bucket in the histogram. The range for the bucket is [low,
 * high). The bucket also keeps a count of the objects in histogram.
 */
public class HistogramBucket implements Comparable<HistogramBucket> {
  private final double low;
  private final double high;

  private double count;

  public HistogramBucket(double low, double high) {
    if (low >= high) {
      throw new IllegalArgumentException(
          String.format("The low %s should be higher than high %s", low, high));
    }
    this.low = low;
    this.high = high;
    this.count = 0;
  }

  public void increment(double incr) {
    this.count += incr;
  }

  public boolean hasOverlap(HistogramBucket bucket) {
    return bucket.high > low && bucket.low < high;
  }

  public boolean contains(double value) {
    return value >= low && value < high;
  }

  public double getLow() {
    return low;
  }

  public double getHigh() {
    return high;
  }

  public double getCount() {
    return count;
  }

  @Override
  public int compareTo(@Nonnull HistogramBucket bucket) {
    if (hasOverlap(bucket)) return 0;
    else if (bucket.high <= low) return 1;
    else return -1;
  }

  public int compareTo(double value) {
    if (value < low) return 1;
    else if (value >= high) return -1;
    else return 0;
  }

  public String toString() {
    return String.format("HistogramBucket low:%f, high:%f, count:%f", low, high, count);
  }
  // TODO: Consider adding "overlap" projection for merge?
}
