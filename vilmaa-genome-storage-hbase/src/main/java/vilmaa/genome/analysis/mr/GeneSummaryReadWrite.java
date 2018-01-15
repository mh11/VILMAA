package vilmaa.genome.analysis.mr;


import vilmaa.genome.analysis.models.avro.GeneSummary;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by mh719 on 28/02/2017.
 */
public class GeneSummaryReadWrite {
    private final SpecificDatumReader<GeneSummary> reader = new SpecificDatumReader<>(GeneSummary.class);
    private final SpecificDatumWriter<GeneSummary> writer = new SpecificDatumWriter<>(GeneSummary.class);
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    private BinaryDecoder decode = DecoderFactory.get().binaryDecoder(new byte[0], null);


    public byte[] write(GeneSummary object) {
        try {
            out.reset();
            BinaryEncoder ben = EncoderFactory.get().binaryEncoder(out, encoder);
            writer.write(object, ben);
            ben.flush();
            return out.toByteArray();
        } catch (IOException e){
            throw new IllegalStateException("Problems writing GeneSummary {} " + object, e);
        }
    }

    public GeneSummary read(byte[] input, GeneSummary reuse) {
        try {
            return reader.read(reuse, DecoderFactory.get().binaryDecoder(input, decode));
        } catch (IOException e){
            throw new IllegalStateException("Problems reading Genesummary from {} " + input, e);
        }
    }
}
