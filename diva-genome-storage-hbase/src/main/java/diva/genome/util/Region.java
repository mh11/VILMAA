package diva.genome.util;

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

    public boolean overlap(int position) {
        return position >=getMinPosition() && position <= getMaxPosition();
    }

    public boolean overlap(Region other) {
        return this.getMinPosition() <= other.getMaxPosition() && other.getMinPosition() <= this.getMaxPosition();
    }

    public boolean coveredBy(Region other) {
        return other.getMinPosition() <= this.getMinPosition() && other.getMaxPosition() >= this.getMaxPosition();
    }
}
