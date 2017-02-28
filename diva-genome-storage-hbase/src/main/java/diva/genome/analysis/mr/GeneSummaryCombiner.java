package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryCombiner extends Reducer<Text, ImmutableBytesWritable, Text, ImmutableBytesWritable> {

    private GeneSummaryReadWrite readWrite;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        readWrite = new GeneSummaryReadWrite();
    }

    @Override
    protected void reduce(Text key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter("DIVA", "combine").increment(1);
        GeneSummary geneSummary = combine(values);
        context.write(key, new ImmutableBytesWritable(readWrite.write(geneSummary)));
    }

    public GeneSummary combine(Iterable<ImmutableBytesWritable> values) {
        Set<Integer> cases = new HashSet<>();
        Set<Integer> ctl = new HashSet<>();
        AtomicReference<GeneSummary> tmp = new AtomicReference<>();
        values.forEach(gs -> {
            GeneSummary read = readWrite.read(gs.get(), tmp.get());
            cases.addAll(read.getCases());
            ctl.addAll(read.getControls());
            tmp.set(read);
        });
        // reuse
        GeneSummary summary = tmp.get();
        summary.getCases().clear();
        summary.getCases().addAll(cases);
        summary.getControls().clear();
        summary.getControls().addAll(ctl);
        return summary;
    }
}
