package org.apache.accumulo.storagehandler.predicate.compare;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

public class DoubleCompareTest {

    private static DoubleCompare doubleCompare;

    @BeforeClass
    public static void setup() {
        doubleCompare = new DoubleCompare();
        byte[] db = new byte[8];
        ByteBuffer.wrap(db).putDouble(10.5d);
        doubleCompare.init(db);
    }

    public static byte[] getBytes(double val) {
        byte [] dBytes = new byte[8];
        ByteBuffer.wrap(dBytes).putDouble(val);
        BigDecimal bd = doubleCompare.serialize(dBytes);
        assertEquals(bd.doubleValue(), val, bd.doubleValue()/10e6);
        return dBytes;
    }

    @Test
    public void equal() {
        Equal equalObj = new Equal(doubleCompare);
        byte[] val = getBytes(10.5d);
        assertTrue(equalObj.accept(val));
    }

    @Test
    public void notEqual() {
        NotEqual notEqualObj = new NotEqual(doubleCompare);
        byte [] val = getBytes(11.0d);
        assertTrue(notEqualObj.accept(val));

        val = getBytes(10.5d);
        assertFalse(notEqualObj.accept(val));

    }

    @Test
    public void greaterThan() {
        GreaterThan greaterThanObj = new GreaterThan(doubleCompare);
        byte [] val = getBytes(11.0d);

        assertTrue(greaterThanObj.accept(val));

        val = getBytes(4.5d);
        assertFalse(greaterThanObj.accept(val));

        val = getBytes(10.5d);
        assertFalse(greaterThanObj.accept(val));
    }

    @Test
    public void greaterThanOrEqual() {
        GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(doubleCompare);

        byte [] val = getBytes(11.0d);

        assertTrue(greaterThanOrEqualObj.accept(val));

        val = getBytes(4.0d);
        assertFalse(greaterThanOrEqualObj.accept(val));

        val = getBytes(10.5d);
        assertTrue(greaterThanOrEqualObj.accept(val));
    }

    @Test
    public void lessThan() {

        LessThan lessThanObj = new LessThan(doubleCompare);

        byte [] val = getBytes(11.0d);

        assertFalse(lessThanObj.accept(val));

        val = getBytes(4.0d);
        assertTrue(lessThanObj.accept(val));

        val = getBytes(10.5d);
        assertFalse(lessThanObj.accept(val));

    }

    @Test
    public void lessThanOrEqual() {

        LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(doubleCompare);

        byte [] val = getBytes(11.0d);

        assertFalse(lessThanOrEqualObj.accept(val));

        val = getBytes(4.0d);
        assertTrue(lessThanOrEqualObj.accept(val));

        val = getBytes(10.5d);
        assertTrue(lessThanOrEqualObj.accept(val));
    }

    @Test
    public void like() {
        try {
            Like likeObj = new Like(doubleCompare);
            assertTrue(likeObj.accept(new byte[]{}));
            fail("should not accept");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Like not supported for " + doubleCompare.getClass().getName()));
        }
    }

    @Test
    public void invalidSerialization() {
        try {
            byte[] badVal = new byte[4];
            ByteBuffer.wrap(badVal).putInt(1);
            doubleCompare.serialize(badVal);
            fail("Should fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(" occurred trying to build double value"));
        }
    }
}
