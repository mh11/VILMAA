package htsjdk.tribble.util.popgen;

import org.junit.Test;

/**
 * Created by mh719 on 27/03/2017.
 */
public class HardyWeinbergCalculationTest {
    @Test(expected = ArithmeticException.class)
    public void hwCalculateFail() throws Exception {
        HardyWeinbergCalculation.hwCalculate(0, 0, 0);
    }
}