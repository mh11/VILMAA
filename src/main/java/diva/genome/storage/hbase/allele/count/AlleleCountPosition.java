package diva.genome.storage.hbase.allele.count;

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

}
