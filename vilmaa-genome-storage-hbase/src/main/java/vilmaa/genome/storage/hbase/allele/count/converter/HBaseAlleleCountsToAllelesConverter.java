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

package vilmaa.genome.storage.hbase.allele.count.converter;

import com.google.common.collect.BiMap;
import vilmaa.genome.analysis.models.variant.stats.VariantStatistics;
import vilmaa.genome.storage.hbase.allele.count.AbstractHBaseAlleleCountsConverter;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.stats.AlleleStatsCalculator;
import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import vilmaa.genome.storage.models.alleles.avro.Genotypes;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.Score;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vilmaa.genome.storage.models.alleles.avro.VariantStats;

import java.util.*;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toMap;

/**
 * Created by mh719 on 24/02/2017.
 */
public class HBaseAlleleCountsToAllelesConverter  extends AbstractHBaseAlleleCountsConverter<AlleleVariant.Builder> {

    private final AlleleCountToGenotypes alleleCountToGenotypes;
    private Logger logger = LoggerFactory.getLogger(HBaseAlleleCountsToAllelesConverter.class);
    private volatile Set<Integer> loadedSamples = null;

    public HBaseAlleleCountsToAllelesConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        super(studyConfiguration, genomeHelper);
        Set<Integer> indexedSamples = StudyConfiguration.getIndexedSamples(this.studyConfiguration).inverse().keySet();
        alleleCountToGenotypes = new AlleleCountToGenotypes(new HashSet<>(indexedSamples));
    }

    @Override
    protected void addStatistics(AlleleVariant.Builder filled, String studyName, Map<String, VariantStatistics> statsMap) {
        HashMap<String, VariantStats> map = new HashMap(statsMap.size());
        statsMap.forEach((key, val) -> map.put(key, buildStats(val)));
        filled.setStats(map);
    }

    private VariantStats buildStats(VariantStatistics stats) {
        org.opencb.biodata.models.variant.avro.VariantStats impl = stats.getImpl();
        VariantStats.Builder builder = VariantStats.newBuilder();
        builder.setRefAlleleCount(impl.getRefAlleleCount());
        builder.setAltAlleleCount(impl.getAltAlleleCount());
        Map<String, Integer> gtCounts = impl.getGenotypesCount().entrySet().stream()
                .collect(toMap(e -> e.getKey().toGenotypeString(), e -> e.getValue()));
        builder.setGenotypesCount(gtCounts);
        builder.setMaf(impl.getMaf());
        builder.setMgf(impl.getMgf());
        builder.setMafAllele(impl.getMafAllele());
        builder.setNumSamples(impl.getNumSamples());
        if (stats.getHw() != null && stats.getHw().getPValue() != null) {
            builder.setHwe(stats.getHw().getPValue().floatValue());
        } else {
            builder.setHwe((float) AlleleStatsCalculator.calcHw(gtCounts));
        }
        builder.setOverallPassrate(ObjectUtils.firstNonNull(stats.getOverallPassRate(), -1f));
        return builder.build();
    }

    @Override
    protected void addAnnotation(AlleleVariant.Builder filled, VariantAnnotation variantAnnotation) {
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

    private vilmaa.genome.storage.models.alleles.avro.VariantAnnotation buildAnnotation(VariantAnnotation variantAnnotation) {

        vilmaa.genome.storage.models.alleles.avro.VariantAnnotation.Builder builder = vilmaa.genome.storage.models.alleles.avro.VariantAnnotation.newBuilder();

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
    protected AlleleVariant.Builder doConvert(Variant variant, AlleleCountPosition bean) {
        ensureSamples();
        // add basic variant info
        AlleleVariant.Builder builder = AlleleVariant.newBuilder()
                .setChromosome(variant.getChromosome())
                .setStart(variant.getStart())
                .setEnd(variant.getEnd())
                .setReference(variant.getReference())
                .setAlternate(variant.getAlternate())
                .setType(variant.getType());

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
        builder.setNotPass(bean.getNotPass());
        // add Allele Count
        builder.setGenotypes(buildGenotypes(variant, bean));
        return builder;
    }

    private Set<Integer> removeOrDefault(Map<String, Set<Integer>> map, String key, Set<Integer> def) {
        Set<Integer> set = map.remove(key);
        if (Objects.isNull(set)) {
            return def;
        }
        return set;
    }

    private Genotypes buildGenotypes(Variant variant, AlleleCountPosition bean) {
        // Build GTs
        AlleleCountToGenotypes.GenotypeCollection genotypeCollection =
                this.alleleCountToGenotypes.convert(bean, loadedSamples, variant, null);
        /* 0/0 0/1 1/1 and . GTs */
        Map<String, Set<Integer>> genotypeToSamples = genotypeCollection.getGenotypeToSamples();
        Integer homRefCount = removeOrDefault(genotypeToSamples, Genotype.HOM_REF, Collections.emptySet()).size();
        List<Integer> hets = new ArrayList<>(removeOrDefault(genotypeToSamples, Genotype.HET_REF, Collections.emptySet()));
        List<Integer> homVar = new ArrayList<>(removeOrDefault(genotypeToSamples, Genotype.HOM_VAR, Collections.emptySet()));
        List<Integer> noCall = new ArrayList<>(removeOrDefault(genotypeToSamples, Genotype.NOCALL, Collections.emptySet()));
        Collections.sort(hets);
        Collections.sort(homVar);
        Collections.sort(noCall);
        /* Other GTs */
        Map<String, List<Integer>> otherGenotypes = new HashMap<>();
        genotypeToSamples.forEach((k,v) -> {
            List<Integer> ids = new ArrayList<>(v);
            Collections.sort(ids);
            otherGenotypes.put(k, ids);
        });
        /* Allele Index */
        List<String> alleleBases = genotypeCollection.getAlleles();
        // Build object
        Genotypes.Builder builder = Genotypes.newBuilder();
        builder.setHomRefCount(homRefCount)
                .setNoCall(noCall)
                .setHet(hets)
                .setHomAlt(homVar)
                .setOtherGenotypes(otherGenotypes)
                .setAlleleBases(alleleBases);
        return builder.build();
    }

}
