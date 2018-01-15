package htsjdk.tribble.util.popgen;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;

/**
 * Created by mh719 on 27/03/2017.
 */
public class HardyWeinbergCalculationTest {

    public Variant extractVariantFromVariantRowKey(byte[] variantRowKey) {
        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0);
        String chromosome = (String) PVarchar.INSTANCE.toObject(variantRowKey, 0, chrPosSeparator, PVarchar.INSTANCE);

        Integer intSize = PUnsignedInt.INSTANCE.getByteSize();
        int position = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey, chrPosSeparator + 1, intSize, PUnsignedInt.INSTANCE);
        int referenceOffset = chrPosSeparator + 1 + intSize;
        int refAltSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0, referenceOffset);
        String reference;
        String alternate;
        if (refAltSeparator < 0) {
            reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, variantRowKey.length - referenceOffset,
                    PVarchar.INSTANCE);
            alternate = "";
        } else {
            reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, refAltSeparator - referenceOffset,
                    PVarchar.INSTANCE);
            alternate = (String) PVarchar.INSTANCE.toObject(variantRowKey, refAltSeparator + 1,
                    variantRowKey.length - (refAltSeparator + 1), PVarchar.INSTANCE);
        }
        try {
            return new Variant(chromosome, position, reference, alternate);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Problems creating variant using [chr:"
                    + chromosome + ", pos:" + position + ", ref:" + reference + ", alt:" + alternate + "];[hexstring:"
                    + Bytes.toHex(variantRowKey) + "]", e);
        }
    }

    @Test(expected = ArithmeticException.class)
    public void hwCalculateFail() throws Exception {
        HardyWeinbergCalculation.hwCalculate(0, 0, 0);
    }
}