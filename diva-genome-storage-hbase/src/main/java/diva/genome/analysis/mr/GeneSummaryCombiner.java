package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneKey;
import diva.genome.analysis.models.avro.GeneSummary;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryCombiner extends Reducer<GeneKey, GeneSummary, GeneKey, GeneSummary> {

    @Override
    protected void reduce(GeneKey key, Iterable<GeneSummary> values, Context context) throws IOException, InterruptedException {
        context.getCounter("DIVA", "combine").increment(1);
        Set<Integer> cases = new HashSet<>();
        Set<Integer> ctl = new HashSet<>();
        String ensId = key.getEnsemblGeneId();
        values.forEach(gs -> {
            cases.addAll(gs.getCases());
            ctl.addAll(gs.getControls());
        });
        GeneSummary geneSummary = GeneSummary.newBuilder()
                .setEnsemblGeneId(ensId)
                .setCases(new ArrayList<>(cases))
                .setControls(new ArrayList<>(ctl))
                .build();
        context.write(key, geneSummary);
    }
}
