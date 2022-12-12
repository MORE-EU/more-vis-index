package eu.more2020.visual.domain;

/**
 * Closed-open, [), interval on the integer number line.
 */
public interface TimeInterval extends Comparable<TimeInterval> {

    /**
     * Returns the starting point of this interval
     */
    long getFrom();

    /**
     * Returns the ending point of this interval
     * <p>
     * The interval does not include this point.
     */
    long getTo();

    /**
     * Returns the length of this.
     */
    default long length() {
        return getTo() - getFrom();
    }


    default boolean contains(long x) {
        return (getFrom() > x && getTo() < x) || getFrom() == x || getTo() == x;
    }


    /**
     * Returns if this interval is adjacent to the specified interval.
     * <p>
     * Two intervals are adjacent if either one ends where the other starts.
     *
     * @param other - the interval to compare this one to
     * @return if this interval is adjacent to the specified interval.
     */
    default boolean isAdjacent(TimeInterval other) {
        return getFrom() == other.getTo() || getTo() == other.getFrom();
    }

    default boolean overlaps(TimeInterval other) {
        return getTo() > other.getFrom() && other.getTo() > getFrom();
    }

    default boolean encloses(TimeInterval other) {
        return (getFrom() <= (other.getFrom()) && this.getTo() >= (other.getTo()));
    }


    default int compareTo(TimeInterval o) {
        if (getFrom() > o.getFrom()) {
            return 1;
        } else if (getFrom() < o.getFrom()) {
            return -1;
        } else if (getTo() > o.getTo()) {
            return 1;
        } else if (getTo() < o.getTo()) {
            return -1;
        } else {
            return 0;
        }
    }
}
