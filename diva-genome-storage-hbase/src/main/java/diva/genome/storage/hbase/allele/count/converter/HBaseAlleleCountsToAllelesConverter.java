package diva.genome.storage.hbase.allele.count.converter;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.count.AbstractHBaseAlleleCountsConverter;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.models.alleles.avro.AlleleCount;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.NO_CALL;

/**
 * Created by mh719 on 24/02/2017.
 */
public class HBaseAlleleCountsToAllelesConverter  extends AbstractHBaseAlleleCountsConverter<AllelesAvro> {

    private Logger logger = LoggerFactory.getLogger(HBaseAlleleCountsToAllelesConverter.class);
    private volatile Set<Integer> loadedSamples = null;

    public HBaseAlleleCountsToAllelesConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        super(studyConfiguration, genomeHelper);
    }

    @Override
    protected void addStatistics(AllelesAvro filled, String studyName, Map<String, VariantStats> statsMap) {
        HashMap<String, org.opencb.biodata.models.variant.avro.VariantStats> map = new HashMap(statsMap.size());
        statsMap.forEach((key, val) -> map.put(key, val.getImpl()));
        filled.setStats(map);
    }

    @Override
    protected void addAnnotation(AllelesAvro filled, VariantAnnotation variantAnnotation) {
        filled.setAnnotation(variantAnnotation);
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
    protected AllelesAvro doConvert(Variant variant, AlleleCountPosition bean) {
        ensureSamples();
        // add basic variant info
        AllelesAvro.Builder builder = AllelesAvro.newBuilder()
                .setChromosome(variant.getChromosome())
                .setStart(variant.getStart())
                .setEnd(variant.getEnd())
                .setLengthReference(variant.getLengthReference())
                .setLengthVariant(Math.max(variant.getLength(), variant.getLengthAlternate()))
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
        return builder.build();
    }

    private AlleleCount buildAlleleCount(AlleleCountPosition bean) {
        AlleleCount.Builder builder = AlleleCount.newBuilder();


        Set<Integer> oneRef = new HashSet<>();
        List<Integer> hets = new ArrayList<>();
        List<Integer> homVar = new ArrayList<>();
        List<Integer> oneAlt = new ArrayList<>();
        hets.addAll(bean.getReference().getOrDefault(1, Collections.emptyList()));
        bean.getAlternate().getOrDefault(1, Collections.emptyList()).forEach((k) ->{
            if (oneRef.remove(k)) {
                hets.add(k);// is het
            } else {
                oneAlt.add(k);
            }
        });
        Map<Integer, List<Integer>> alts = new HashMap<>();
        if (!oneAlt.isEmpty()) {
            alts.put(1, oneAlt);
        }
        bean.getAlternate().forEach((k, v) -> {
            if (k == 2) {
                homVar.addAll(v);
            } else if (k < 1 || k > 2) {
                alts.put(k, v);
            }
        });
        Map<Integer, List<Integer>> refs = new HashMap<>();
        if (!oneRef.isEmpty()) {
            refs.put(1, new ArrayList<>(oneRef));
        }
        List<Integer> nocall = new ArrayList<>();
        bean.getReference().forEach((k, v) -> {
            if (k == NO_CALL) {
                nocall.addAll(v);
            } else if (k < 1 || k > 2) {
                refs.put(k, v);
            }
        });


        builder.setReferenceAlleleCounts(refs)
                .setNoCall(nocall)
                .setHet(hets)
                .setHomVar(homVar)
                .setAltAlleleCounts(alts)
                .setOtherAltAlleleCounts(bean.getAltMap());
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
