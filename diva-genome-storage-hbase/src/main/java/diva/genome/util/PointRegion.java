package diva.genome.util;

/**
 * Single position acting as a region
 * Created by mh719 on 17/03/2017.
 */
public class PointRegion<T> extends Region<T> {
    private final int position;

    public PointRegion(T data, int position) {
        super(data);
        this.position = position;
    }

    @Override
    public int getStart() {
        return position;
    }

    @Override
    public int getEnd() {
        return position;
    }
}
