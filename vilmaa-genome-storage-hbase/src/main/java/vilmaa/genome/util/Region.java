package vilmaa.genome.util;

/**
 * Region class to handle regions and calculate if this region overlaps a position or another region <br>
 * The start end end positions are inclusive.
 * Created by mh719 on 17/03/2017.
 */
public abstract class Region<T> {
    private final T data;

    public Region(T data) {
        this.data = data;
    }

    abstract public int getStart();
    abstract public int getEnd();

    public int getMinPosition() {
        return Math.min(getStart(), getEnd());
    }

    public int getMaxPosition() {
        return Math.max(getStart(), getEnd());
    }

    public T getData() {
        return data;
    }

    public boolean sameRegion(Region other) {
        return this.getStart() == other.getStart() && this.getEnd() == other.getEnd();
    }

    public boolean overlap(int position) {
        return overlap(new PointRegion(null, position));
    }

    /**
     * see {@link #overlap(Region, boolean)}
     */
    public boolean overlap(int position, boolean insertionAware) {
        return overlap(new PointRegion(null, position), insertionAware);
    }

    public boolean overlap(Region other) {
        return overlap(other, false);
    }

    public boolean overlap(Region other, boolean insertionAware) {
        if (sameRegion(other)) {
            return true;
        }
        if (insertionAware) {
            return this.getStart() <= other.getMaxPosition() && other.getStart() <= this.getMaxPosition();
        }
        return this.getMinPosition() <= other.getMaxPosition() && other.getMinPosition() <= this.getMaxPosition();
    }

    public boolean coveredBy(Region other) {
        return other.getMinPosition() <= this.getMinPosition() && other.getMaxPosition() >= this.getMaxPosition();
    }

    /**
     * Length of region - can be negative in case of end < start. If start == end
     * @return int length
     */
    public int getLength() {
        int len = getEnd() - getStart();
        if (len >= 0) {
            len += 1;
        }
        return len;
    }

    /**
     * Size of region - always positive. (max position - min position). If start == end, size is 1;
     * @return int size
     */
    public int getCoveredPositions() {
        return (getMaxPosition() - getMinPosition())+1;
    }
}
