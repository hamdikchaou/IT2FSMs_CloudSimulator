package fcm;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.math3.ml.clustering.Clusterable;

/**
 * A simple implementation of {@link Clusterable} for points with double coordinates.
 * @since 3.2
 */
public class DoublePoint implements Clusterable, Serializable {

    /** Serializable version identifier. */
    private static final long serialVersionUID = 3946024775784901369L;

    /** Point coordinates. */
    private final double[] point;

    /**
     * Build an instance wrapping an double array.
     * <p>
     * The wrapped array is referenced, it is <em>not</em> copied.
     *
     * @param point the n-dimensional point in double space
     */
    public DoublePoint(final double[] point) {
        this.point = point;
    }

    /**
     * Build an instance wrapping an integer array.
     * <p>
     * The wrapped array is copied to an internal double array.
     *
     * @param point the n-dimensional point in integer space
     */
    public DoublePoint(final int[] point) {
        this.point = new double[point.length];
        for ( int i = 0; i < point.length; i++) {
            this.point[i] = point[i];
        }
    }

    /** {@inheritDoc} */
    public double[] getPoint() {
        return point;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof DoublePoint)) {
            return false;
        }
        return Arrays.equals(point, ((DoublePoint) other).point);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(point);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Arrays.toString(point);
    }

}
