package diva.genome.storage.hbase.allele.count.converter;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;

import java.util.*;

import static diva.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator.DEL_SYMBOL;
import static diva.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator.INS_SYMBOL;
import static org.junit.Assert.*;

/**
 * Created by mh719 on 24/04/2017.
 */
public class AlleleCountToGenotypesTest {
    @Test
    public void convert() throws Exception {

        // 1-9: REF / ALT
        // 10-19: Sec Alt G
        // 20-  : INDELs
        Set<Integer> indexed = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 20, 21));
        AlleleCountToGenotypes alleleCountToGenotypes = new AlleleCountToGenotypes(indexed);
        AlleleCountPosition position = new AlleleCountPosition();
        position.getReference().put(-1, Arrays.asList(7));
        position.getReference().put(1, Arrays.asList(1, 2, 5, 6, 10, 20));
        position.getAlternate().put(1, Arrays.asList(1, 2, 5, 8, 12));
        position.getAlternate().put(2, Arrays.asList(3));

        position.getAltMap().computeIfAbsent("G", x -> new HashMap<>()).put(1, Arrays.asList(10,12));
        position.getAltMap().computeIfAbsent("G", x -> new HashMap<>()).put(2, Arrays.asList(11));
        position.getAltMap().computeIfAbsent(DEL_SYMBOL, x -> new HashMap<>()).put(1, Arrays.asList(20));
        position.getAltMap().computeIfAbsent(INS_SYMBOL, x -> new HashMap<>()).put(2, Arrays.asList(21));

        Set<Integer> samples = new HashSet<>(Arrays.asList(1, 2, 3, 4, 6, 7, 8, 10, 11, 12, 20, 21));
        Variant variant = new Variant("1:100:A:T");
        AlleleCountToGenotypes.GenotypeCollection gts = alleleCountToGenotypes.convert(position, samples, variant, null);
        Map<String, Integer> alleleIdx = new HashMap<>();
        for (int i = 0; i < gts.getAlleles().size(); i++) {
            alleleIdx.put(gts.getAlleles().get(i), i);
        }

        assertEquals("HET", new HashSet(Arrays.asList(1,2)), gts.getGenotypeToSamples().get(Genotype.HET_REF));
        assertEquals("HOM_ALT", new HashSet(Arrays.asList(3)), gts.getGenotypeToSamples().get(Genotype.HOM_VAR));
        assertEquals("HOM_REF", new HashSet(Arrays.asList(4, 21)), gts.getGenotypeToSamples().get(Genotype.HOM_REF));
        assertEquals("Hemi", new HashSet(Arrays.asList(6)), gts.getGenotypeToSamples().get("0"));
        assertEquals("Hemi", new HashSet(Arrays.asList(8)), gts.getGenotypeToSamples().get("1"));
        assertEquals("NoCall", new HashSet(Arrays.asList(7)), gts.getGenotypeToSamples().get(Genotype.NOCALL));

        // SecAlt
        Integer gIdx = alleleIdx.get("G");
        assertEquals("SecAlt G", new HashSet(Arrays.asList(10)), gts.getGenotypeToSamples().get("0/" + gIdx));
        assertEquals("SecAlt G", new HashSet(Arrays.asList(11)), gts.getGenotypeToSamples().get(gIdx + "/" + gIdx));
        assertEquals("SecAlt T/G", new HashSet(Arrays.asList(12)), gts.getGenotypeToSamples().get("1/" + gIdx));

        Integer delIdx = alleleIdx.get(DEL_SYMBOL);
        assertEquals("SecAlt Del", new HashSet(Arrays.asList(20)), gts.getGenotypeToSamples().get("0/" + delIdx));

    }

}