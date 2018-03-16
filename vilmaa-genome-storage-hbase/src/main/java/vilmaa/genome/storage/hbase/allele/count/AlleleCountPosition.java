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

package vilmaa.genome.storage.hbase.allele.count;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 03/02/2017.
 */
public class AlleleCountPosition {
    private final List<Integer> notPass = new ArrayList<>();
    private final List<Integer> pass = new ArrayList<>();
    private final Map<Integer, List<Integer>> reference = new HashMap<>();
    private final Map<Integer, List<Integer>> alternate = new HashMap<>();
    private final Map<String, Map<Integer, List<Integer>>> altMap = new HashMap<>();

    public AlleleCountPosition() {
        // do nothing
    }

    public AlleleCountPosition(AlleleCountPosition copy, Set<Integer> valid) {
        this();
        if (null != valid && valid.isEmpty()) {
            throw new IllegalStateException("Please provide IDs to filter on");
        }
        this.pass.addAll(copyAndFilter(copy.pass, valid));
        this.notPass.addAll(copyAndFilter(copy.notPass, valid));
        copy.reference.forEach((k, v) -> this.reference.put(k, copyAndFilter(v, valid)));
        copy.alternate.forEach((k, v) -> this.alternate.put(k, copyAndFilter(v, valid)));

        copy.altMap.forEach((vid, map) -> {
            Map<Integer, List<Integer>> currMap = this.altMap.computeIfAbsent(vid, k -> new HashMap<>());
            map.forEach((k, v) -> currMap.put(k, copyAndFilter(v, valid)));
        });
    }

    public AlleleCountPosition(AlleleCountPosition copy) {
        this(copy, null);
    }

    private List<Integer> copyAndFilter(List<Integer> from, Set<Integer> valid) {
        if (from.isEmpty()) {
            return new ArrayList<>();
        }
        if (null == valid) {
            return new ArrayList<>(from);
        }
        return from.stream().filter(i -> valid.contains(i)).collect(Collectors.toList());
    }


    public List<Integer> getNotPass() {
        return notPass;
    }

    public List<Integer> getPass() {
        return pass;
    }

    public Map<Integer, List<Integer>> getAlternate() {
        return alternate;
    }
    public Map<Integer, List<Integer>> getReference() {
        return reference;
    }
    public Map<String, Map<Integer, List<Integer>>> getAltMap() {
        return altMap;
    }

    public void filterIds(Set<Integer> query) {
        filterIds(query, getNotPass());
        getReference().values().forEach(v -> filterIds(query, v));
        getAlternate().values().forEach(v -> filterIds(query, v));
        getAltMap().values().forEach(vm -> vm.values().forEach(v -> filterIds(query, v)));
    }

    private void filterIds(Set<Integer> query, List<Integer> target) {
        Set<Integer> collect = target.stream().filter(i -> query.contains(i)).collect(Collectors.toSet());
        target.clear();
        target.addAll(collect);
    }

    public String toDebugString() {
        return "Reference: " + this.reference + "\nalternate: " + this.alternate + "\naltmap: " + this.altMap;
    }

}
