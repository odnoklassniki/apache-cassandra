/*
 * @(#) ReentrantByteArrayInputStream.java
 * Created 22.06.2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class ReentrantByteArrayInputStream extends ByteArrayInputStream
{

    /**
     * @param arg0
     */
    public ReentrantByteArrayInputStream(byte[] arg0)
    {
        super(arg0);
    }

    /**
     * @param buf
     * @param offset
     * @param length
     */
    public ReentrantByteArrayInputStream(byte[] buf, int offset, int length)
    {
        super(buf, offset, length);
    }

    public void reset(byte[] buf, int offset, int length)
    {
        this.buf = buf;
        this.mark = 0;
        this.count = Math.min(offset + length, buf.length);;
        this.pos = offset;
    }
    
    public void reset(byte[] buf)
    {
        reset(buf,0,buf.length);
    }
}
