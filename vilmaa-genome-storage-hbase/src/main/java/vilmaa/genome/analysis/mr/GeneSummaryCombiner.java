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

package vilmaa.genome.analysis.mr;

import vilmaa.genome.analysis.models.avro.GeneSummary;
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
        init();
    }

    void init() {
        readWrite = new GeneSummaryReadWrite();
    }

    @Override
    protected void reduce(Text key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter("vilmaa", "combine").increment(1);
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
