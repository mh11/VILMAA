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

package vilmaa.genome.analysis.models.variant.stats;

/**
 * Created by mh719 on 28/03/2017.
 */
public class VariantStatistics extends org.opencb.biodata.models.variant.stats.VariantStats{

    private volatile Float overallPassRate;
    private volatile Float callRate;
    private volatile Float passRate;

    public VariantStatistics() {
        super();
    }

    public VariantStatistics(org.opencb.biodata.models.variant.stats.VariantStats stats) {
        super(stats.getImpl());
    }

    public Float getOverallPassRate() {
        return overallPassRate;
    }

    public void setOverallPassRate(Float overallPassRate) {
        this.overallPassRate = overallPassRate;
    }

    public Float getCallRate() {
        return callRate;
    }

    public void setCallRate(Float callRate) {
        this.callRate = callRate;
    }

    public Float getPassRate() {
        return passRate;
    }

    public void setPassRate(Float passRate) {
        this.passRate = passRate;
    }

}
