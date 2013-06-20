package org.apache.cassandra.db;

import junit.framework.Assert;
import org.apache.cassandra.cache.ByteArrayStreamInput;
import org.apache.cassandra.cache.ByteArrayStreamOutput;
import org.apache.cassandra.cache.MemoryWrapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author roman.antipin
 *         Date: 19.06.13
 */
public class SerializationStreamingTest {

    private final static byte[] TEST_BYTE_ARRAY = {(byte)-1, (byte)12, (byte)124, (byte)-34, (byte)111, (byte)-3};
    private final static String TEST_STRING = "Hello world, Привет мир!!!";
    private final static byte TEST_BYTE = 11;
    private final static int TEST_INT = 12345;
    private final static long TEST_LONG = -112L;
    private final static short TEST_SHORT = 32456;
    private final static double TEST_DOUBLE = 0.45678;
    private final static float TEST_FLOAT = 123.2452f;

    @Test
    public void testWriteToMemory() throws IOException {


        ByteArrayStreamOutput output = new ByteArrayStreamOutput();
        output.write(TEST_BYTE_ARRAY, 2, 3);
        output.writeByte(TEST_BYTE);
        output.writeInt(TEST_INT);
        output.writeLong(TEST_LONG);
        output.writeShort(TEST_SHORT);
        output.writeBoolean(true);
        output.writeBoolean(false);
        output.writeDouble(TEST_DOUBLE);
        output.writeFloat(TEST_FLOAT);
        output.writeUTF(TEST_STRING);
        output.write(TEST_BYTE_ARRAY);

        MemoryWrapper memoryWrapper = new MemoryWrapper(output.getCount(), output.getBuffer());

        ByteArrayStreamInput input = new ByteArrayStreamInput(memoryWrapper);

        byte[] buffer1 = new byte[] {(byte)-1, (byte)12, (byte)0, (byte)0, (byte)0, (byte)-3};
        input.readFully(buffer1, 2, 3);
        Assert.assertTrue(Arrays.equals(buffer1, TEST_BYTE_ARRAY));

        Assert.assertEquals(input.readByte(), TEST_BYTE);
        Assert.assertEquals(input.readInt(), TEST_INT);
        Assert.assertEquals(input.readLong(), TEST_LONG);
        Assert.assertEquals(input.readShort(), TEST_SHORT);
        Assert.assertEquals(input.readBoolean(), true);
        Assert.assertEquals(input.readBoolean(), false);
        Assert.assertEquals(Double.compare(input.readDouble(), TEST_DOUBLE), 0);
        Assert.assertEquals(Double.compare(input.readFloat(), TEST_FLOAT), 0);
        Assert.assertEquals(input.readUTF(), TEST_STRING);

        byte[] buffer2 = new byte[TEST_BYTE_ARRAY.length];
        input.readFully(buffer2);
        Assert.assertTrue(Arrays.equals(buffer2, TEST_BYTE_ARRAY));
    }
}
