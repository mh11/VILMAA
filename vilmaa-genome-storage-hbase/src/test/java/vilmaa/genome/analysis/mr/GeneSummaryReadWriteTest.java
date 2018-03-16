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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 28/02/2017.
 */
public class GeneSummaryReadWriteTest {

    private GeneSummaryReadWrite writeable;

    @Before
    public void setUp() throws Exception {
        writeable = new GeneSummaryReadWrite();
    }

    @Test
    public void roundTripTest() throws Exception {

        GeneSummary abc = GeneSummary.newBuilder()
                .setCases(Arrays.asList(1, 2, 3))
                .setControls(Arrays.asList(4, 5,6))
                .setEnsemblGeneId("abc")
                .setEnsemblTranscriptId("XXX")
                .build();
        byte[] write = writeable.write(abc);

        GeneSummary read1 = writeable.read(write, null);
        assertEquals(abc, read1);


        GeneSummary other = GeneSummary.newBuilder()
                .setCases(new ArrayList<>(Arrays.asList(2)))
                .setControls(new ArrayList<>(Arrays.asList(1)))
                .setEnsemblGeneId("xxx")
                .setEnsemblTranscriptId("XXX")
                .build();

        assertNotEquals(abc, other);

        assertEquals(abc, writeable.read(write, other));
        assertEquals(abc, other);
        assertEquals(abc, writeable.read(write, read1));

    }

    @Test
    public void modifyTest() throws Exception {

        GeneSummary abc = GeneSummary.newBuilder()
                .setCases(new ArrayList<>(Arrays.asList(1, 2, 3)))
                .setControls(new ArrayList<>(Arrays.asList(4, 5,6)))
                .setEnsemblGeneId("abc")
                .setEnsemblTranscriptId("XXX")
                .build();
        byte[] first = writeable.write(abc);
        GeneSummary firstOut = writeable.read(first, null);
        assertEquals(abc, firstOut);

        abc.getCases().clear();
        abc.getCases().addAll(Arrays.asList(22,33,44));
        abc.getControls().clear();
        abc.getControls().addAll(Arrays.asList(77,88,99));

        byte[] second = writeable.write(abc);
        GeneSummary secondOut = writeable.read(second, null);

        assertEquals(abc, secondOut);

        assertNotEquals(firstOut, secondOut);

    }

}