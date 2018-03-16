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

/**
 * Created by mh719 on 25/02/2017.
 */
public class VariantConsequence {

    public static Boolean isHigh(String name) {
        switch (name) {
            case "transcript_ablation":
            case "splice_acceptor_variant":
            case "splice_donor_variant":
            case "stop_gained":
            case "frameshift_variant":
            case "stop_lost":
            case "start_lost":
            case "transcript_amplification":
                return true;
            default:
                return false;
        }
    }

    public static Boolean isModerate(String name) {
        switch (name) {
            case "inframe_insertion":
            case "inframe_deletion":
            case "missense_variant":
            case "protein_altering_variant":
            case "regulatory_region_ablation":
                return true;
            default:
                return false;
        }
    }

    public static Boolean isHighOrModerate(String name) {
        return isHigh(name) || isModerate(name);
    }


}
