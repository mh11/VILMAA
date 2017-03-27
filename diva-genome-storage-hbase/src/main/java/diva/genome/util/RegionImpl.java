package diva.genome.util;

/**
 * Region implementation for a start/end. Start can be larger than the end (e.g. INSERTION)
 * Created by mh719 on 17/03/2017.
 */
public class RegionImpl <T> extends Region<T>  {
    private final int start;
    private final int end;

    public RegionImpl(T data, int start, int end) {
        super(data);
        this.start = start;
        this.end = end;
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    public int getEnd() {
        return end;
    }

}
