package diva.genome.storage.hbase.allele.count.converter;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.count.AbstractHBaseAlleleCountsConverter;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.models.alleles.avro.AlleleCount;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import htsjdk.tribble.util.popgen.HardyWeinbergCalculation;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static diva.genome.storage.hbase.allele.count.position.AbstractAlleleCalculator.NO_CALL;
import static java.util.stream.Collectors.*;

/**
 * Created by mh719 on 24/02/2017.
 */
public class HBaseAlleleCountsToAllelesConverter  extends AbstractHBaseAlleleCountsConverter<AllelesAvro.Builder> {

    private Logger logger = LoggerFactory.getLogger(HBaseAlleleCountsToAllelesConverter.class);
    private volatile Set<Integer> loadedSamples = null;

    public HBaseAlleleCountsToAllelesConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        super(studyConfiguration, genomeHelper);
    }

    @Override
    protected void addStatistics(AllelesAvro.Builder filled, String studyName, Map<String, VariantStats> statsMap) {
        HashMap<String, diva.genome.storage.models.alleles.avro.VariantStats> map = new HashMap(statsMap.size());
        statsMap.forEach((key, val) -> map.put(key, buildStats(val)));
        filled.setStats(map);
        double wgs10kmaf = -1;
        if (statsMap.containsKey("WGS10K")) {
            VariantStats stats = statsMap.get("WGS10K");
            if (null != stats) {
                wgs10kmaf = stats.getMaf();
            }
        }
        filled.setMafWgs10k((float) wgs10kmaf);
    }

    private diva.genome.storage.models.alleles.avro.VariantStats buildStats(VariantStats val) {
        org.opencb.biodata.models.variant.avro.VariantStats impl = val.getImpl();
        diva.genome.storage.models.alleles.avro.VariantStats.Builder builder = diva.genome.storage.models.alleles
                .avro.VariantStats.newBuilder();
        builder.setRefAlleleCount(impl.getRefAlleleCount());
        builder.setAltAlleleCount(impl.getAltAlleleCount());
        Map<String, Integer> gtCounts = impl.getGenotypesCount().entrySet().stream()
                .collect(toMap(e -> e.getKey().toGenotypeString(), e -> e.getValue()));
        builder.setGenotypesCount(gtCounts);
        builder.setMaf(impl.getMaf());
        builder.setMgf(impl.getMgf());
        builder.setMafAllele(impl.getMafAllele());
        builder.setNumSamples(impl.getNumSamples());
        if (val.getHw() != null && val.getHw().getPValue() != null) {
            builder.setHwe(val.getHw().getPValue().floatValue());
        } else {
            builder.setHwe((float) calcHw(gtCounts));
        }
        return builder.build();
    }

    private double calcHw(Map<String, Integer> gtCounts) {
        int aa = gtCounts.getOrDefault("1/1", 0);
        int ab = gtCounts.getOrDefault("0/1", 0);
        int bb = gtCounts.getOrDefault("0/0", 0);
        if (aa > bb) { // aa should be rare allele
            int tmp = bb;
            bb = aa;
            aa = tmp;
        }
        if ((aa + ab + bb)  < 1) {
            return -1;
        }
        return HardyWeinbergCalculation.hwCalculate(aa, ab, bb);
    }

    @Override
    protected void addAnnotation(AllelesAvro.Builder filled, VariantAnnotation variantAnnotation) {
        if (null == variantAnnotation) {
            variantAnnotation = new VariantAnnotation();
        }
        filled.setAnnotation(buildAnnotation(variantAnnotation));
        Set<String> csq = new HashSet<>();
        Set<String> ensGeneIds = new HashSet<>();
        Set<String> bioTypes = new HashSet<>();
        isNotNull(variantAnnotation.getConsequenceTypes(), l -> {
            l.forEach(c -> {
                if (StringUtils.isNotEmpty(c.getBiotype())) {
                    bioTypes.add(c.getBiotype());
                }
                c.getSequenceOntologyTerms().forEach(s -> csq.add(s.getName()));
                if (StringUtils.isNotEmpty(c.getEnsemblGeneId())) {
                    ensGeneIds.add(c.getEnsemblGeneId());
                }
            });
        });
        filled.setConsequenceTypes(new ArrayList<>(csq));
        filled.setBioTypes(new ArrayList<>(bioTypes));
        filled.setEnsemblGeneIds(new ArrayList<>(ensGeneIds));

        if (null != variantAnnotation.getFunctionalScore()) {
            OptionalDouble max = variantAnnotation.getFunctionalScore().stream()
                    .filter(s -> s.getSource().equals("cadd_scaled"))
                    .mapToDouble(Score::getScore).max();
            if (max.isPresent()) {
                filled.setCaddScaled((float) max.getAsDouble());
            }
        }
    }

    private diva.genome.storage.models.alleles.avro.VariantAnnotation buildAnnotation(VariantAnnotation variantAnnotation) {

        diva.genome.storage.models.alleles.avro.VariantAnnotation.Builder builder = diva.genome.storage.models
                .alleles.avro.VariantAnnotation.newBuilder();

        isNotNull(variantAnnotation.getId(), StringUtils.EMPTY, h -> builder.setId(h));
        isNotNull(variantAnnotation.getDisplayConsequenceType(), StringUtils.EMPTY, h -> builder.setDisplayConsequenceType(h));

        /* List types */
        isNotNullList(variantAnnotation.getXrefs(), h -> builder.setXrefs(h));
        isNotNullList(variantAnnotation.getHgvs(), h -> builder.setHgvs(h));
        isNotNullList(variantAnnotation.getConsequenceTypes(), l -> builder.setConsequenceTypes(l));
        isNotNullList(variantAnnotation.getPopulationFrequencies(), l -> builder.setPopulationFrequencies(l));
        isNotNullList(variantAnnotation.getConservation(), l -> builder.setConservation(l));
        isNotNullList(variantAnnotation.getFunctionalScore(), l -> builder.setFunctionalScore(l));

        return builder.build();
    }

    private <T> void isNotNullList(List<T> t, Consumer<List<T>> c) {
        List<T> lst = new ArrayList<>();
        if (!Objects.isNull(t)) {
            isNotNull(t, h -> lst.addAll(h));
        }
        c.accept(lst);
    }

    private <T> void isNotNull(T t, Consumer<T> c) {
        if (!Objects.isNull(t)) {
            c.accept(t);
        }
    }

    private <T> void isNotNull(T t, T def, Consumer<T> c) {
        if (Objects.isNull(t)) {
            c.accept(def);
        } else {
            c.accept(t);
        }
    }

    protected void ensureSamples() {
        if (null != loadedSamples) {
            return;
        }
        Set<Integer> sampleIds = new HashSet<>();
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
        LinkedHashMap<String, Integer> returnedSamplesPosition = buildReturnSamplePositionMap();
        returnedSamplesPosition.forEach((name, position) -> sampleIds.add(indexedSamples.get(name)));
        this.loadedSamples = sampleIds;
    }

    @Override
    protected AllelesAvro.Builder doConvert(Variant variant, AlleleCountPosition bean) {
        ensureSamples();
        // add basic variant info
        AllelesAvro.Builder builder = AllelesAvro.newBuilder()
                .setChromosome(variant.getChromosome())
                .setStart(variant.getStart())
                .setEnd(variant.getEnd())
                .setReference(variant.getReference())
                .setAlternate(variant.getAlternate())
                .setType(getType(variant));



        // Filter IDs
        bean.filterIds(loadedSamples);

        // Sample stuff
        builder.setNumberOfSamples(loadedSamples.size());
        Map<String, String> rates = new HashMap<>();
        if (!loadedSamples.isEmpty()) {
            rates.putAll(calculatePassCallRates(bean, loadedSamples.size()));
        }
        builder.setNumberOfSamples(loadedSamples.size());
        builder.setPass(new Integer(rates.getOrDefault("PASS", "0")));
        builder.setPassRate(new Float(rates.getOrDefault("PR", "0")));
        builder.setCallRate(new Float(rates.getOrDefault("CR", "0")));
        builder.setOverallPassRate(new Float(rates.getOrDefault("OPR", "0")));
        builder.setNotPass(bean.getNotPass());
        // add Allele Count
        builder.setAlleleCount(buildAlleleCount(bean));
        return builder;
    }

    private AlleleCount buildAlleleCount(AlleleCountPosition bean) {
        AlleleCount.Builder builder = AlleleCount.newBuilder();


        Set<Integer> oneRef = new HashSet<>();
        List<Integer> hets = new ArrayList<>();
        List<Integer> homVar = new ArrayList<>();
        List<Integer> oneAlt = new ArrayList<>();
        oneRef.addAll(bean.getReference().getOrDefault(1, Collections.emptyList()));
        bean.getAlternate().getOrDefault(1, Collections.emptyList()).forEach((k) ->{
            if (oneRef.remove(k)) {
                hets.add(k);// is het
            } else {
                oneAlt.add(k);
            }
        });
        Map<String, List<Integer>> alts = new HashMap<>();
        if (!oneAlt.isEmpty()) {
            alts.put("1", oneAlt);
        }
        bean.getAlternate().forEach((k, v) -> {
            if (k == 2) {
                homVar.addAll(v);
            } else if (k < 1 || k > 2) {
                alts.put(k.toString(), v);
            }
        });
        Map<String, List<Integer>> refs = new HashMap<>();
        if (!oneRef.isEmpty()) {
            refs.put("1", new ArrayList<>(oneRef));
        }
        List<Integer> nocall = new ArrayList<>();
        bean.getReference().forEach((k, v) -> {
            if (k == NO_CALL) {
                nocall.addAll(v);
            } else if (k < 1 || k > 2) {
                refs.put(k.toString(), v);
            }
        });

        builder.setReferenceAlleleCounts(refs)
                .setNoCall(nocall)
                .setHet(hets)
                .setHomVar(homVar)
                .setAltAlleleCounts(alts)
                .setOtherAltAlleleCounts( // Map from <String<Integer<List<Integer>>> to <String<String<List<Integer>>>
                        bean.getAltMap().entrySet().stream().collect(
                                toMap(e -> e.getKey(),
                                        e -> e.getValue().entrySet().stream().collect(
                                                toMap(f -> f.getKey().toString(), f -> f.getValue())))));
        return builder.build();
    }

    private VariantType getType(Variant variant) {
        if (!variant.getType().equals(VariantType.INDEL)) {
            return variant.getType();
        }
        if (variant.getStart() > variant.getEnd()) {
            return VariantType.INSERTION;
        }
        return VariantType.DELETION;
    }
}
