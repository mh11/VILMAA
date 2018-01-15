package vilmaa.genome.storage.hbase.filter;

import vilmaa.genome.storage.hbase.VariantHbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by mh719 on 07/12/2017.
 */
public class ExportPosRefAltFilter implements IHbaseVariantFilter {
    public static final String CONFIG_ALLELE_FILTER_POSREFALT="vilmaa.genome.storage.allele.posrefalt.file";

    private Logger log = LoggerFactory.getLogger(this.getClass());
    private volatile Map<String,Map<Integer,Map<String,Set<String>>>> positions = new HashMap<>();

    public ExportPosRefAltFilter() {
        // empty
    }

    public static ExportPosRefAltFilter build(Configuration conf) {
        ExportPosRefAltFilter filter = new ExportPosRefAltFilter();
        String file = conf.get(CONFIG_ALLELE_FILTER_POSREFALT, "");
        if (!StringUtils.isEmpty(file)) {
            filter.loadFile(file);
        }
        return filter;
    }

    private void normalise(Variant variant) {

        // remove leading character for INDELs.
        if (variant.getReference().charAt(0) == variant.getAlternate().charAt(0)) {
            variant.setReference(variant.getReference().substring(1));
            variant.setAlternate(variant.getAlternate().substring(1));
            variant.setStart(variant.getStart()+1);
        }
        // recalc END
        variant.setEnd(variant.getStart() + variant.getLengthReference() - 1);
        // Infer type
        VariantHbaseUtil.inferAndSetType(variant);
    }

    public void loadFile(String file) {
        getLog().info("Load file " + file);
        Variant variant = new Variant("n", 10, "n", "n");
        try(BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line = null;
            while(!Objects.isNull(line = in.readLine())) {
                line = StringUtils.remove(line, "\n");
                if (line.length() < 1) {
                    continue; // skip empty lines
                }
                String[] split = line.split("\t");
                if (split.length != 4) {
                    throw new IOException("Expected 4 fields, only found " + split.length + " for >" + line + "<");
                }
                variant.setChromosome(split[0]);
                variant.setStart(Integer.valueOf(split[1]));
                variant.setReference(new String(split[2].toCharArray()).toUpperCase());
                variant.setAlternate(new String(split[3].toCharArray()).toUpperCase());

                // set type and remove leading base for INDELs.
                normalise(variant);

                positions.computeIfAbsent(variant.getChromosome(), k -> new HashMap<>())
                        .computeIfAbsent(variant.getStart(), k -> new HashMap<>())
                        .computeIfAbsent(variant.getReference(), k -> new HashSet<>())
                        .add(variant.getAlternate());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problems loading file " + file, e);
        }
        int cnt = positions.values().stream().mapToInt(v -> v.size()).sum();
        getLog().info("Loaded " + cnt + " positions");
    }

    @Override
    public boolean pass(Result value, Variant variant) {
        Map<Integer, Map<String, Set<String>>> posMap = positions.get(variant.getChromosome());
        if (Objects.isNull(posMap)) {
            return false;
        }
        Map<String, Set<String>> vars = posMap.get(variant.getStart());
        if (Objects.isNull(vars)) {
            return false;
        }

        Set<String> alts = vars.get(variant.getReference());
        if (Objects.isNull(alts)) {
            return false;
        }
        return alts.contains(variant.getAlternate());
    }

    @Override
    public boolean hasFilters() {
        return !positions.isEmpty();
    }

    public Logger getLog() {
        return log;
    }
}
