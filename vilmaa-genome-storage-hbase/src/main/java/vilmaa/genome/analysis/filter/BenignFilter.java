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
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.ProteinVariantAnnotation;
import org.opencb.biodata.models.variant.avro.Score;

import java.util.List;

/**
 * Created by mh719 on 26/02/2017.
 */
public class BenignFilter  extends AbstractFilter<AlleleVariant> {

    public static final String SIFT = "sift";
    public static final String POLYPHEN = "polyphen";
    public static final String SIFT_TOLERATED = "tolerated";
    public static final String POLY_BENIGN = "benign";

    @Override
    public Boolean doTest(AlleleVariant AlleleVariant) {
        if (AlleleVariant.getAnnotation() == null || AlleleVariant.getAnnotation().getConsequenceTypes() == null) {
            return  false;
        }
        return hasBenign(AlleleVariant.getAnnotation().getConsequenceTypes());
    }

    public static boolean hasBenign(List<ConsequenceType> annotations) {
        if (null == annotations) {
            return false;
        }
        return annotations.stream().anyMatch(a -> isBenign(a.getProteinVariantAnnotation()));
    }

    public static boolean isBenign(ProteinVariantAnnotation prot) {
        if (null == prot) {
            return false;
        }
        List<Score> scores = prot.getSubstitutionScores();
        if (null == scores) {
            return false;
        }
        boolean siftTolerated = false;
        boolean polyBenign = false;
        for (Score score : scores) {
            if (StringUtils.equals(score.getSource(), SIFT)
                    && StringUtils.equals(score.getDescription(), SIFT_TOLERATED)) {
                siftTolerated = true;
            } else if (StringUtils.equals(score.getSource(), POLYPHEN)
                    && StringUtils.equals(score.getDescription(), POLY_BENIGN)) {
                polyBenign = true;
            }
        }
        return siftTolerated && polyBenign;
    }

}
