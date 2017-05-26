package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
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