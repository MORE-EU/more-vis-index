package eu.more2020.visual.domain;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collector;

/**
 * A state object for collecting statistics such as count, min, max, sum, and
 * average.
 *
 * <p>This class is designed to work with (though does not require)
 * {@linkplain java.util.stream streams}. For example, you can compute
 * summary statistics on a stream of doubles with:
 * <pre> {@code
 * DoubleSummaryStatistics stats = doubleStream.collect(DoubleSummaryStatistics::new,
 *                                                      DoubleSummaryStatistics::accept,
 *                                                      DoubleSummaryStatistics::combine);
 * }</pre>
 *
 * <p>{@code DoubleSummaryStatistics} can be used as a
 * {@linkplain java.util.stream.Stream#collect(Collector) reduction}
 * target for a {@linkplain java.util.stream.Stream stream}. For example:
 *
 * <pre> {@code
 * DoubleSummaryStatistics stats = people.stream()
 *     .collect(Collectors.summarizingDouble(Person::getWeight));
 * }</pre>
 * <p>
 * This computes, in a single pass, the count of people, as well as the minimum,
 * maximum, sum, and average of their weights.
 *
 * @implNote This implementation is not thread safe. However, it is safe to use
 * {@link java.util.stream.Collectors#summarizingDouble(java.util.function.ToDoubleFunction)
 * Collectors.summarizingDouble()} on a parallel stream, because the parallel
 * implementation of {@link java.util.stream.Stream#collect Stream.collect()}
 * provides the necessary partitioning, isolation, and merging of results for
 * safe and efficient parallel execution.
 * @since 1.8
 */
public class Stats implements Consumer<DataPoint> {

    private int count = 0;
    private double[] sums;
    private double[] minValues;
    private long[] minTimestamps;
    private double[] maxValues;
    private long[] maxTimestamps;


    public Stats(List<Integer> measures) {
        int length = measures.size();
        sums = new double[length];
        minValues = new double[length];
        minTimestamps = new long[length];
        maxValues = new double[length];
        maxTimestamps = new long[length];

        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
    }

    public void clear() {
        count = 0;
        Arrays.fill(sums, 0d);
        Arrays.fill(minValues, Double.POSITIVE_INFINITY);
        Arrays.fill(minTimestamps, -1l);
        Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
        Arrays.fill(maxTimestamps, -1l);
    }



        /**
         * Records another datapoint into the summary information.
         *
         * @param dataPoint the dataPoint
         */
    @Override
    public void accept(DataPoint dataPoint) {
        ++count;
        for (int i = 0; i < dataPoint.getValues().length; i++) {
            double value = dataPoint.getValues()[i];
            sums[i] += value;
            minValues[i] = Math.min(minValues[i], value);
            if (minValues[i] == value) {
                minTimestamps[i] = dataPoint.getTimestamp();
            }
            maxValues[i] = Math.max(maxValues[i], value);
            if (maxValues[i] == value) {
                maxTimestamps[i] = dataPoint.getTimestamp();
            }
        }
    }

    /**
     * Combines the state of another {@code DoubleSummaryStatistics} into this
     * one.
     *
     * @param other another {@code DoubleSummaryStatistics}
     * @throws NullPointerException if {@code other} is null
     */
    public void combine(Stats other) {
        count += other.count;
        for (int i = 0; i < sums.length; i++) {
            sums[i] += other.sums[i];
            minValues[i] = Math.min(minValues[i], other.minValues[i]);
            if (minValues[i] == other.minValues[i]) {
                minTimestamps[i] = other.minTimestamps[i];
            }
            maxValues[i] = Math.min(maxValues[i], other.maxValues[i]);
            if (maxValues[i] == other.maxValues[i]) {
                maxTimestamps[i] = other.maxTimestamps[i];
            }
        }
    }


    /**
     * Return the count of data points aggregated.
     *
     * @return the count of data points
     */
    public final int getCount() {
        return count;
    }

    /**
     * Returns the sum of values recorded, or zero if no values have been
     * recorded.
     *
     * <p> The value of a floating-point sum is a function both of the
     * input values as well as the order of addition operations. The
     * order of addition operations of this method is intentionally
     * not defined to allow for implementation flexibility to improve
     * the speed and accuracy of the computed result.
     * <p>
     * In particular, this method may be implemented using compensated
     * summation or other technique to reduce the error bound in the
     * numerical sum compared to a simple summation of {@code double}
     * values.
     * <p>
     * Because of the unspecified order of operations and the
     * possibility of using differing summation schemes, the output of
     * this method may vary on the same input values.
     *
     * <p>Various conditions can result in a non-finite sum being
     * computed. This can occur even if the all the recorded values
     * being summed are finite. If any recorded value is non-finite,
     * the sum will be non-finite:
     *
     * <ul>
     *
     * <li>If any recorded value is a NaN, then the final sum will be
     * NaN.
     *
     * <li>If the recorded values contain one or more infinities, the
     * sum will be infinite or NaN.
     *
     * <ul>
     *
     * <li>If the recorded values contain infinities of opposite sign,
     * the sum will be NaN.
     *
     * <li>If the recorded values contain infinities of one sign and
     * an intermediate sum overflows to an infinity of the opposite
     * sign, the sum may be NaN.
     *
     * </ul>
     *
     * </ul>
     * <p>
     * It is possible for intermediate sums of finite values to
     * overflow into opposite-signed infinities; if that occurs, the
     * final sum will be NaN even if the recorded values are all
     * finite.
     * <p>
     * If all the recorded values are zero, the sign of zero is
     * <em>not</em> guaranteed to be preserved in the final sum.
     *
     * @return the sum of values, or zero if none
     * @apiNote Values sorted by increasing absolute magnitude tend to yield
     * more accurate results.
     */
    public final double[] getSums() {
        return sums;
    }

    /**
     * Returns the minimum recorded value, {@code Double.NaN} if any recorded
     * value was NaN or {@code Double.POSITIVE_INFINITY} if no values were
     * recorded. Unlike the numerical comparison operators, this method
     * considers negative zero to be strictly smaller than positive zero.
     *
     * @return the minimum recorded value, {@code Double.NaN} if any recorded
     * value was NaN or {@code Double.POSITIVE_INFINITY} if no values were
     * recorded
     */
    public final double[] getMinValues() {
        return minValues;
    }

    /**
     * Returns the maximum recorded value, {@code Double.NaN} if any recorded
     * value was NaN or {@code Double.NEGATIVE_INFINITY} if no values were
     * recorded. Unlike the numerical comparison operators, this method
     * considers negative zero to be strictly smaller than positive zero.
     *
     * @return the maximum recorded value, {@code Double.NaN} if any recorded
     * value was NaN or {@code Double.NEGATIVE_INFINITY} if no values were
     * recorded
     */
    public final double[] getMaxValues() {
        return maxValues;
    }

    /**
     * Returns the arithmetic mean of values recorded, or zero if no
     * values have been recorded.
     *
     * <p> The computed average can vary numerically and have the
     * special case behavior as computing the sum; see {@link #getSum}
     * for details.
     *
     * @return the arithmetic mean of values, or zero if none
     * @apiNote Values sorted by increasing absolute magnitude tend to yield
     * more accurate results.
     */
    public final double[] getAverageValues() {
        return getCount() > 0 ? Arrays.stream(sums).map(sum -> sum / count).toArray() : sums;
    }

    public long[] getMinTimestamps() {
        return minTimestamps;
    }

    public long[] getMaxTimestamps() {
        return maxTimestamps;
    }
}
