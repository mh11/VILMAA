/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
