/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.obs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

/** An "open" BitSet implementation that allows direct access to the array of words
 * storing the bits.
 * <p/>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs
 * is part of the interface.  This allows efficient implementation of other algorithms
 * by someone other than the author.  It also allows one to efficiently implement
 * alternate serialization or interchange formats.
 * <p/>
 * <code>OpenBitSet</code> is faster than <code>java.util.BitSet</code> in most operations
 * and *much* faster at calculating cardinality of sets and results of set operations.
 * It can also handle sets of larger cardinality (up to 64 * 2**32-1)
 * <p/>
 * The goals of <code>OpenBitSet</code> are the fastest implementation possible, and
 * maximum code reuse.  Extra safety and encapsulation
 * may always be built on top, but if that's built in, the cost can never be removed (and
 * hence people re-implement their own version in order to get better performance).
 * If you want a "safe", totally encapsulated (and slower and limited) BitSet
 * class, use <code>java.util.BitSet</code>.
 */

public class OpenBitSet implements IBitSet {
    protected long[][] bits;
    protected int wlen;   // number of words (elements) used in the array
    /**
     * length of bits[][] page in long[] elements. 
     * Choosing unform size for all sizes of bitsets fight fragmentation for very large
     * bloom filters (2G and more)
     */
    protected static final int PAGE_SIZE= 4096; 

    /** Constructs an OpenBitSet large enough to hold numBits.
     *
     * @param numBits
     */
    public OpenBitSet(long numBits) 
    {
        this(numBits,true);
    }

    public OpenBitSet(long numBits, boolean allocatePages) 
    {
        wlen= BitUtil.bits2words(numBits);    

        bits = new long[getPageCount()][];

        if (allocatePages)
        {
            for (int allocated=0,i=0;allocated<wlen;allocated+=PAGE_SIZE,i++)
                bits[i]=new long[PAGE_SIZE];
        }
    }

    public OpenBitSet() {
        this(64);
    }

    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return PAGE_SIZE;
    }

    public int getPageCount()
    {
        return wlen / PAGE_SIZE + 1;
    }

    public long[] getPage(int pageIdx)
    {
        return bits[pageIdx];
    }

    public void setPage(int pageIdx, long[] page)
    {
        bits[pageIdx] = page;
    }

    /** Contructs an OpenBitset from a BitSet
     */
    public OpenBitSet(BitSet bits) {
        this(bits.length());
    }

    /** Returns the current capacity in bits (1 greater than the index of the last bit) */
    public long capacity() { return ((long)wlen) << 6; }

    /**
     * Returns the current capacity of this set.  Included for
     * compatibility.  This is *not* equal to {@link #cardinality}
     */
    public long size() {
        return capacity();
    }

    // @Override -- not until Java 1.6
    public long length() {
        return capacity();
    }

    /** Expert: gets the number of longs in the array that are in use */
    public long sizeInWords() { return wlen; }

    /** Returns true or false for the specified bit index
     */
    public boolean get(long index) {
        int i = (int)(index >> 6);             // div 64
        if (i>=wlen) return false;
        int bit = (int)index & 0x3f;           // mod 64
        long bitmask = 1L << bit;
        // TODO perfectionist one can implement this using bit operations
        return (bits[i / PAGE_SIZE][i % PAGE_SIZE ] & bitmask) != 0;
    }

    /** sets a bit, expanding the set size if necessary */
    public void set(long index) {
        int wordNum = expandingWordNum(index);
        int bit = (int)index & 0x3f;
        long bitmask = 1L << bit;
        bits[ wordNum / PAGE_SIZE ][ wordNum % PAGE_SIZE ] |= bitmask;
    }


    /** Sets a range of bits, expanding the set size if necessary
     *
     * @param startIndex lower index
     * @param endIndex one-past the last bit to set
     */
    public void set(long startIndex, long endIndex) {
        if (endIndex <= startIndex) return;

        int startWord = (int)(startIndex>>6);

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord   = expandingWordNum(endIndex-1);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

        if (startWord == endWord) {
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] |= (startmask & endmask);
            return;
        }

        assert startWord / PAGE_SIZE == endWord / PAGE_SIZE : "cross page sets not suppotred at all - they are not used";

        bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] |= startmask;
        Arrays.fill(bits[ startWord / PAGE_SIZE], (startWord+1) % PAGE_SIZE , endWord % PAGE_SIZE , -1L);
        bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] |= endmask;
    }


    protected int expandingWordNum(long index) {
        int wordNum = (int)(index >> 6);
        if (wordNum>=wlen) {
            throw new IndexOutOfBoundsException( wordNum+">"+wlen );
        }
        return wordNum;
    }


    /** clears a bit.
     * The index should be less than the OpenBitSet size.
     */
    public void fastClear(int index) {
        int wordNum = index >> 6;
        int bit = index & 0x03f;
        long bitmask = 1L << bit;
        bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
        // hmmm, it takes one more instruction to clear than it does to set... any
        // way to work around this?  If there were only 63 bits per word, we could
        // use a right shift of 10111111...111 in binary to position the 0 in the
        // correct place (using sign extension).
        // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
        // by the JVM into a native instruction.
        // bits[word] &= Long.rotateLeft(0xfffffffe,bit);
    }

    /** clears a bit.
     * The index should be less than the OpenBitSet size.
     */
    public void fastClear(long index) {
        int wordNum = (int)(index >> 6); // div 64
        int bit = (int)index & 0x3f;     // mod 64
        long bitmask = 1L << bit;
        bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
    }

    /** clears a bit, allowing access beyond the current set size without changing the size.*/
    public void clear(long index) {
        int wordNum = (int)(index >> 6); // div 64
        if (wordNum>=wlen) return;
        int bit = (int)index & 0x3f;     // mod 64
        long bitmask = 1L << bit;
        bits[wordNum / PAGE_SIZE][wordNum % PAGE_SIZE] &= ~bitmask;
    }

    /** Clears a range of bits.  Clearing past the end does not change the size of the set.
     *
     * @param startIndex lower index
     * @param endIndex one-past the last bit to clear
     */
    public void clear(int startIndex, int endIndex) {
        if (endIndex <= startIndex) return;

        int startWord = (startIndex>>6);
        if (startWord >= wlen) return;

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord   = ((endIndex-1)>>6);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

        // invert masks since we are clearing
        startmask = ~startmask;
        endmask = ~endmask;

        if (startWord == endWord) {
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] &= (startmask | endmask);
            return;
        }


        bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE]  &= startmask;

        int middle = Math.min(wlen, endWord);
        if (startWord / PAGE_SIZE == middle / PAGE_SIZE)
        {
            Arrays.fill(bits[startWord/PAGE_SIZE], (startWord+1) % PAGE_SIZE, middle % PAGE_SIZE, 0L);
        } else
        {
            while (++startWord<middle)
                bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] = 0L;
        }
        if (endWord < wlen) {
            bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
        }
    }


    /** Clears a range of bits.  Clearing past the end does not change the size of the set.
     *
     * @param startIndex lower index
     * @param endIndex one-past the last bit to clear
     */
    public void clear(long startIndex, long endIndex) {
        if (endIndex <= startIndex) return;

        int startWord = (int)(startIndex>>6);
        if (startWord >= wlen) return;

        // since endIndex is one past the end, this is index of the last
        // word to be changed.
        int endWord   = (int)((endIndex-1)>>6);

        long startmask = -1L << startIndex;
        long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

        // invert masks since we are clearing
        startmask = ~startmask;
        endmask = ~endmask;

        if (startWord == endWord) {
            bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] &= (startmask | endmask);
            return;
        }

        bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE]  &= startmask;

        int middle = Math.min(wlen, endWord);
        if (startWord / PAGE_SIZE == middle / PAGE_SIZE)
        {
            Arrays.fill(bits[startWord/PAGE_SIZE], (startWord+1) % PAGE_SIZE, middle % PAGE_SIZE, 0L);
        } else
        {
            while (++startWord<middle)
                bits[startWord / PAGE_SIZE][startWord % PAGE_SIZE] = 0L;
        }
        if (endWord < wlen) {
            bits[endWord / PAGE_SIZE][endWord % PAGE_SIZE] &= endmask;
        }
    }

    /** @return the number of set bits */
    public long cardinality() 
    {
        long bitCount = 0L;
        for (int i=getPageCount();i-->0;)
            bitCount+=BitUtil.pop_array(bits[i],0,wlen);

        return bitCount;
    }

    /** returns true if both sets have the same bits set */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OpenBitSet)) return false;
        OpenBitSet a;
        OpenBitSet b = (OpenBitSet)o;
        // make a the larger set.
        if (b.wlen > this.wlen) {
            a = b; b=this;
        } else {
            a=this;
        }

        int aPageSize = PAGE_SIZE;
        int bPageSize = PAGE_SIZE;

        // check for any set bits out of the range of b
        for (int i=a.wlen-1; i>=b.wlen; i--) {
            if (a.bits[i/aPageSize][i % aPageSize]!=0) return false;
        }

        for (int i=b.wlen-1; i>=0; i--) {
            if (a.bits[i/aPageSize][i % aPageSize] != b.bits[i/bPageSize][i % bPageSize]) return false;
        }

        return true;
    }


    @Override
    public int hashCode() {
        // Start with a zero hash and use a mix that results in zero if the input is zero.
        // This effectively truncates trailing zeros without an explicit check.
        long h = 0;
        for (int i = wlen; --i>=0;) {
            h ^= bits[i / PAGE_SIZE][i % PAGE_SIZE];
            h = (h << 1) | (h >>> 63); // rotate left
        }
        // fold leftmost bits into right and add a constant to prevent
        // empty sets from returning 0, which is too common.
        return (int)((h>>32) ^ h) + 0x98761234;
    }

    @Override
    public void close()
    {

    }

    
    @Override
    public void serialize( DataOutput dos ) throws IOException
    {
        int pageCount = getPageCount();
        long wordsLength = sizeInWords();
        
        for (int p = 0;p<pageCount;p++)
        {
            long[] bits = getPage(p);
            for ( int i = 0; i < PAGE_SIZE && wordsLength-- > 0; i++ )
                dos.writeLong(bits[i]);
        }
        
    }
    
    public void deserialize( DataInput dis ) throws IOException {
        int pageCount = getPageCount();
        long words = sizeInWords();
        
        for (int p = 0;p<pageCount;p++)
        {
            long[] bits = getPage(p);
            for ( int i = 0; i < PAGE_SIZE && words-- > 0; i++ )
                bits[i] = dis.readLong();
        }
    }

    @Override
    public DataOutput newDataOutput()
    {
        throw new UnsupportedOperationException("Method OpenBitSet.newDataOutput() is not supported");
    }
}


