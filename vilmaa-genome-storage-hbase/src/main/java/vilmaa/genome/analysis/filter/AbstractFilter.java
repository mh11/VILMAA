package vilmaa.genome.analysis.filter;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 28/02/2017.
 */
public abstract class AbstractFilter <T> implements Function<T, Boolean>, Predicate<T> {
    @Override
    public Boolean apply(T t) {
        return doTest(t);
    }

    @Override
    public boolean test(T t) {
        return doTest(t);
    }

    public abstract Boolean doTest(T t);


}
