package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryCombiner extends Reducer<Text, GeneSummary, Text, GeneSummary> {

    @Override
    protected void reduce(Text key, Iterable<GeneSummary> values, Context context) throws IOException, InterruptedException {
        context.getCounter("DIVA", "combine").increment(1);
        GeneSummary geneSummary = combine(key, values);

        context.write(key, geneSummary);
    }

    public GeneSummary combine(Text key, Iterable<GeneSummary> values) {
        Set<Integer> cases = new HashSet<>();
        Set<Integer> ctl = new HashSet<>();
        values.forEach(gs -> {
            cases.addAll(gs.getCases());
            ctl.addAll(gs.getControls());
        });
        return GeneSummary.newBuilder()
                .setEnsemblGeneId(key.toString())
                .setCases(new ArrayList<>(cases))
                .setControls(new ArrayList<>(ctl))
                .build();
    }
}
