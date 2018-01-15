package vilmaa.genome.storage.hbase.allele.count.region;

import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.util.PointRegion;
import vilmaa.genome.util.Region;
import vilmaa.genome.util.RegionImpl;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by mh719 on 25/05/2017.
 */
public class AlleleRegionCalculatorTest {
    Map<String, Integer> snToSid = new HashMap<>();
    Variant varA = buildVariant("1:13:A:T","A","0/1");
    Variant varB = buildVariant("1:9-21:.","B","0/0");
    Variant varC = buildVariant("1:14::AGC","C","0/1");
    Variant varC2 = buildVariant("1:13:A:G","C","0/1");
    Variant varD = buildVariant("1:13:AGC:","D","0/1");
    Variant varE = buildVariant("1:11-18:.","E","./.");
    Variant varF = buildVariant("1:13:A:G","F","1/1");
    Variant varG = buildVariant("1:13:A:T","G","1/2");

    @Before
    public void prepare() {
        snToSid.put("A",21);
        snToSid.put("B",31);
        snToSid.put("C",41);
        snToSid.put("D",51);
        snToSid.put("E",61);
        snToSid.put("F",71);
        snToSid.put("G",81);
        varG.getStudy("1").setSecondaryAlternates(Arrays.asList(new AlternateCoordinate("1",13,13, "A", "G", VariantType.SNV)));
    }

    @Test
    public void fromSingleToMerged() {
        HBaseAlleleRegionTransfer transfer = new HBaseAlleleRegionTransfer(new HashSet<>(snToSid.values()));
        AlleleRegionCalculator calculator = new AlleleRegionCalculator("1", snToSid, 10, 20);

        calculator.addVariant(varA);
        calculator.addVariant(varB);
        calculator.addVariant(varC);
        calculator.addVariant(varC2);
        calculator.addVariant(varD);
        calculator.addVariant(varE);
        calculator.addVariant(varF);
        calculator.addVariant(varG);

        calculator.fillNoCalls(snToSid.keySet(), 10, 20);
        calculator.onlyLeaveSparseRepresentation(10,20, false, false);

        transfer.transfer("1", new RegionImpl(null, 10, 15), calculator.getStore(), (k,v) -> {
            System.out.println("k = " + k);
            System.out.println("v = " + v);
            switch (k.toString()) {
                case "1:13:A:T" :
                    assertEquals(1, v.getAlternate().size());
                    assertEquals(2, v.getAlternate().get(1).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("A"), snToSid.get("G"))), new HashSet(v.getAlternate().get(1)));
                    assertEquals(2, v.getReference().size());
                    assertEquals(3, v.getReference().get(1).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("A"), snToSid.get("C"), snToSid.get("D"))), new HashSet(v.getReference().get(1)));
                    assertEquals(Arrays.asList(snToSid.get("E")), v.getReference().get(-1));

                    assertEquals(2, v.getAltMap().size());
                    assertEquals(2, v.getAltMap().get("G").size());
                    assertEquals(1, v.getAltMap().get("*").size());
                    assertEquals(Arrays.asList(snToSid.get("C"), snToSid.get("G")), v.getAltMap().get("G").get(1));
                    assertEquals(Arrays.asList(snToSid.get("F")), v.getAltMap().get("G").get(2));
                    assertEquals(Arrays.asList(snToSid.get("D")), v.getAltMap().get("*").get(1));
                    break;
                case "1:14:-:AGC":
                    assertEquals(1, v.getAlternate().size());
                    assertEquals(1, v.getAlternate().get(1).size());
                    assertEquals(2, v.getReference().size());
                    assertEquals(2, v.getReference().get(1).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("C"), snToSid.get("D"))), new HashSet(v.getReference().get(1)));
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("E"), snToSid.get("A"), snToSid.get("F"), snToSid.get("G"))), new HashSet(v.getReference().get(-1))); // A as no-call, since this position is not covered by a call!!!.

                    assertEquals(Arrays.asList(snToSid.get("D")), v.getAltMap().get("*").get(1));
                    if (v.getAltMap().containsKey("+")) {
                        assertTrue(v.getAltMap().get("+").isEmpty());
                        v.getAltMap().remove("+");
                    }
                    assertEquals(1, v.getAltMap().size());

                    break;
                case "1:13:A:G":
                    assertEquals(2, v.getAlternate().size());
                    assertEquals(2, v.getAlternate().get(1).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("C"), snToSid.get("G"))), new HashSet(v.getAlternate().get(1)));
                    assertEquals(1, v.getAlternate().get(2).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("F"))), new HashSet(v.getAlternate().get(2)));
                    assertEquals(2, v.getReference().size());
                    assertEquals(3, v.getReference().get(1).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("A"), snToSid.get("C"), snToSid.get("D"))), new HashSet(v.getReference().get(1)));
                    assertEquals(Arrays.asList(snToSid.get("E")), v.getReference().get(-1));

                    assertEquals(2, v.getAltMap().size());
                    assertEquals(1, v.getAltMap().get("T").size());
                    assertEquals(1, v.getAltMap().get("*").size());
                    assertEquals(Arrays.asList(snToSid.get("A"), snToSid.get("G")), v.getAltMap().get("T").get(1));
                    assertEquals(Arrays.asList(snToSid.get("D")), v.getAltMap().get("*").get(1));
                    break;
                case "1:13:AGC:-":
                    assertEquals(1, v.getAlternate().size());
                    assertEquals(1, v.getAlternate().get(1).size());
                    assertEquals(2, v.getReference().size());
                    assertEquals(1, v.getReference().get(1).size());
                    assertEquals(new HashSet(Arrays.asList(snToSid.get("D"))), new HashSet(v.getReference().get(1)));
                    assertEquals(Arrays.asList(snToSid.get("E")), v.getReference().get(-1));
//
                    assertEquals(0, v.getAltMap().entrySet().stream().map(e -> e.getValue().size()).mapToInt(e -> e).sum());
                    break;
                default:
                    throw new RuntimeException("Unkown: " + k);

            }
        });

    }

    @Test
    public void addVariant() throws Exception {

        AlleleRegionCalculator calculator = new AlleleRegionCalculator("1", snToSid, 10, 20);

        calculator.addVariant(varA);
        calculator.addVariant(varB);

        AlleleRegionStore store = calculator.getStore();
        List<Region<AlleleInfo>> reglst = store.getVariation(new RegionImpl(null, 10,20));
        assertEquals(1, reglst.size());
        Region<AlleleInfo> areg = reglst.get(0);
        assertEquals(varA.getStart().intValue(), areg.getStart());
        AlleleInfo ai = areg.getData();
        assertEquals(varA.getReference(), ai.getId()[0]);
        assertEquals(varA.getAlternate(), ai.getId()[1]);

        assertEquals(1, ai.getSampleIds().size());

        assertEquals(2, store.getReference(new PointRegion(0, 13)).size());

        assertEquals(1, store.getAll(new PointRegion(0, 12)).size());
        assertEquals(3, store.getAll(new PointRegion(0, 13)).size());


        calculator.addVariant(varC);
        calculator.addVariant(varD);

        assertEquals(3,store.getVariation(new RegionImpl(null, 10,20)).size());
        assertEquals(3,store.getVariation(new PointRegion(null, 13)).size());
        assertEquals(4, store.getReference(new PointRegion(0, 13)).size());

        calculator.addVariant(varC2);
        assertEquals(4,store.getVariation(new PointRegion(null, 13)).size());

    }

    private Variant buildVariant(String str, String name, String gt) {
        Variant var = new Variant(str);
        StudyEntry se = new StudyEntry("1");
        Map<String, Integer> spos = new HashMap<>();
        spos.put(name,0);
        se.setSamplesPosition(spos);
        se.setFormat(Arrays.asList(VariantMerger.GT_KEY));
        se.setSamplesData(Arrays.asList(Arrays.asList(gt)));
        var.setStudies(Arrays.asList(se));
        return var;
    }

}