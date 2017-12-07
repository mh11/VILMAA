package diva.genome.storage.hbase.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by mh719 on 07/12/2017.
 */
public class ExportPosRefAltFilter implements IHbaseVariantFilter {
    public static final String CONFIG_ALLELE_FILTER_POSREFALT="diva.genome.storage.allele.posrefalt.file";

    private Logger log = LoggerFactory.getLogger(this.getClass());
    private volatile Map<String,Map<Integer,String[]>> positions = new HashMap<>();

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

    public void loadFile(String file) {
        getLog().info("Load file " + file);
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
                String chrom = split[0];
                Integer pos = Integer.valueOf(split[1]);
                String ref = new String(split[2].toCharArray());
                String alt = new String(split[3].toCharArray());
                positions.computeIfAbsent(chrom, k -> new HashMap<>())
                        .put(pos, new String[]{ref,alt});
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problems loading file " + file, e);
        }
        int cnt = positions.values().stream().mapToInt(v -> v.size()).sum();
        getLog().info("Loaded " + cnt + " positions");
    }

    @Override
    public boolean pass(Result value, Variant variant) {
        Map<Integer, String[]> posMap = positions.get(variant.getChromosome());
        if (Objects.isNull(posMap)) {
            return false;
        }
        String[] alts = posMap.get(variant.getStart());
        if (Objects.isNull(alts)) {
            return false;
        }
        return StringUtils.equals(alts[0], variant.getReference())
                && StringUtils.equals(alts[1], variant.getAlternate());
    }

    @Override
    public boolean hasFilters() {
        return !positions.isEmpty();
    }

    public Logger getLog() {
        return log;
    }
}
