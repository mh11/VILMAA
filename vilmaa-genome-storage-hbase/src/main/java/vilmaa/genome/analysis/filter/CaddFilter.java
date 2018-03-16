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

import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.avro.Score;

import java.util.List;

/**
 * Created by mh719 on 26/02/2017.
 */
public class CaddFilter extends AbstractFilter<AlleleVariant> {
    public static final int CADD_CUTOFF = 15;
    public static final String CADD_SCALED = "cadd_scaled";


    @Override
    public Boolean doTest(AlleleVariant AlleleVariant) {
        if (null == AlleleVariant || AlleleVariant.getAnnotation() == null
                || AlleleVariant.getAnnotation().getFunctionalScore() == null) {
            return false;
        }
        return hasHighCadd(AlleleVariant.getAnnotation().getFunctionalScore());
    }

    public boolean hasHighCadd(List<Score> functionalScores){
        if (null == functionalScores || functionalScores.isEmpty()) {
            return false;
        }
        for (Score score : functionalScores) {
            if (isHighCadd(score)) {
                return true;
            }
        }
        return false;
    }

    public boolean isHighCadd(Score score) {
        if (null == score) {
            return false;
        }
        Double value = score.getScore();
        if (value == null || value < CADD_CUTOFF) { // null or less than 15 (no matter which score)
            return false;
        }
        return StringUtils.equals(score.getSource(), CADD_SCALED);
    }
}
