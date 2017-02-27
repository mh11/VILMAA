package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter("DIVA", "combine").increment(1);
        context.write(key, new IntWritable(combineInt(values)));
//        GeneSummary geneSummary = combine(key, values);
//
//        context.write(key, geneSummary);
    }

    public Integer combineInt(Iterable<IntWritable> values) {
        AtomicInteger cnt = new AtomicInteger(0);
        values.forEach(i -> cnt.addAndGet(i.get()));
        return cnt.get();

//        values.stream().
//        return GeneSummary.newBuilder()
//                .setEnsemblGeneId(key.toString())
//                .setCases(new ArrayList<>(cases))
//                .setControls(new ArrayList<>(ctl))
//                .build();
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
