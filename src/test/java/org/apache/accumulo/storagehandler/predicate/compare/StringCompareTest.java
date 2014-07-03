package org.apache.accumulo.storagehandler.predicate.compare;

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class StringCompareTest {

    private static StringCompare strCompare;


    @BeforeClass
    public static void setup() {
        strCompare = new StringCompare();
        strCompare.init("aaa".getBytes());
    }

    @Test
    public void equal() {
        Equal equalObj = new Equal(strCompare);
        byte[] val = "aaa".getBytes();
        assertTrue(equalObj.accept(val));
    }

    @Test
    public void notEqual() {
        NotEqual notEqualObj = new NotEqual(strCompare);
        byte [] val = "aab".getBytes();
        assertTrue(notEqualObj.accept(val));

        val = "aaa".getBytes();
        assertFalse(notEqualObj.accept(val));

    }

    @Test
    public void greaterThan() {
        GreaterThan greaterThanObj = new GreaterThan(strCompare);
        byte [] val = "aab".getBytes();

        assertTrue(greaterThanObj.accept(val));

        val = "aa".getBytes();
        assertFalse(greaterThanObj.accept(val));

        val = "aaa".getBytes();
        assertFalse(greaterThanObj.accept(val));
    }

    @Test
    public void greaterThanOrEqual() {
        GreaterThanOrEqual greaterThanOrEqualObj = new GreaterThanOrEqual(strCompare);
        byte [] val = "aab".getBytes();

        assertTrue(greaterThanOrEqualObj.accept(val));

        val = "aa".getBytes();
        assertFalse(greaterThanOrEqualObj.accept(val));

        val = "aaa".getBytes();
        assertTrue(greaterThanOrEqualObj.accept(val));
    }

    @Test
    public void lessThan() {

        LessThan lessThanObj = new LessThan(strCompare);

        byte [] val = "aab".getBytes();

        assertFalse(lessThanObj.accept(val));

        val = "aa".getBytes();
        assertTrue(lessThanObj.accept(val));

        val = "aaa".getBytes();
        assertFalse(lessThanObj.accept(val));

    }

    @Test
    public void lessThanOrEqual() {

        LessThanOrEqual lessThanOrEqualObj = new LessThanOrEqual(strCompare);

        byte [] val = "aab".getBytes();

        assertFalse(lessThanOrEqualObj.accept(val));

        val = "aa".getBytes();
        assertTrue(lessThanOrEqualObj.accept(val));

        val = "aaa".getBytes();
        assertTrue(lessThanOrEqualObj.accept(val));
    }

    @Test
    public void like() {
        Like likeObj = new Like(strCompare);
        String condition = "%a";
        assertTrue(likeObj.accept(condition.getBytes()));

        condition = "%a%";
        assertTrue(likeObj.accept(condition.getBytes()));

        condition = "a%";
        assertTrue(likeObj.accept(condition.getBytes()));

        condition = "a%aa";
        assertFalse(likeObj.accept(condition.getBytes()));

        condition = "b%";
        assertFalse(likeObj.accept(condition.getBytes()));

        condition = "%ab%";
        assertFalse(likeObj.accept(condition.getBytes()));

        condition = "%ba";
        assertFalse(likeObj.accept(condition.getBytes()));
    }
}
