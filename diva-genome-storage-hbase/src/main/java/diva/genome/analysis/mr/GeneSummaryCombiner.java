package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryCombiner extends Reducer<AvroKey<Text>, AvroValue<GeneSummary>, AvroKey<Text>, AvroValue<GeneSummary>> {

    @Override
    protected void reduce(AvroKey<Text> key, Iterable<AvroValue<GeneSummary>> values, Context context) throws IOException, InterruptedException {
        context.getCounter("DIVA", "combine").increment(1);
        Set<Integer> cases = new HashSet<>();
        Set<Integer> ctl = new HashSet<>();
        values.forEach(gs -> {
            cases.addAll(gs.datum().getCases());
            ctl.addAll(gs.datum().getControls());
        });
        GeneSummary geneSummary = GeneSummary.newBuilder()
                .setEnsemblGeneId(key.datum().toString())
                .setCases(new ArrayList<>(cases))
                .setControls(new ArrayList<>(ctl))
                .build();

        context.write(key, new AvroValue<>(geneSummary));
    }
}
