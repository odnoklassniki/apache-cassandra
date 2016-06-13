/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.Random;


import sun.misc.Unsafe;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class MurmurHash
{
    public static int hash32(ByteBuffer data, int offset, int length, int seed)
    {
        int m = 0x5bd1e995;
        int r = 24;

        int h = seed ^ length;

        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++)
        {
            int i_4 = i << 2;
            int k = data.get(offset + i_4 + 3);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 2) & 0xff);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 1) & 0xff);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 0) & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // avoid calculating modulo
        int len_m = len_4 << 2;
        int left = length - len_m;

        if (left != 0)
        {
            if (left >= 3)
            {
                h ^= (int) data.get(offset + length - 3) << 16;
            }
            if (left >= 2)
            {
                h ^= (int) data.get(offset + length - 2) << 8;
            }
            if (left >= 1)
            {
                h ^= (int) data.get(offset + length - 1);
            }

            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static long hash64(ByteBuffer key, int offset, int length, long seed)
    {
        long m64 = 0xc6a4a7935bd1e995L;
        int r64 = 47;

        long h64 = (seed & 0xffffffffL) ^ (m64 * length);

        int lenLongs = length >> 3;

        for (int i = 0; i < lenLongs; ++i)
        {
            int i_8 = i << 3;

            long k64 =  ((long)  key.get(offset+i_8+0) & 0xff)      + (((long) key.get(offset+i_8+1) & 0xff)<<8)  +
			            (((long) key.get(offset+i_8+2) & 0xff)<<16) + (((long) key.get(offset+i_8+3) & 0xff)<<24) +
			            (((long) key.get(offset+i_8+4) & 0xff)<<32) + (((long) key.get(offset+i_8+5) & 0xff)<<40) +
			            (((long) key.get(offset+i_8+6) & 0xff)<<48) + (((long) key.get(offset+i_8+7) & 0xff)<<56);
           
            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }

        int rem = length & 0x7;

        switch (rem)
        {
        case 0:
            break;
        case 7:
            h64 ^= (long) key.get(offset + length - rem + 6) << 48;
        case 6:
            h64 ^= (long) key.get(offset + length - rem + 5) << 40;
        case 5:
            h64 ^= (long) key.get(offset + length - rem + 4) << 32;
        case 4:
            h64 ^= (long) key.get(offset + length - rem + 3) << 24;
        case 3:
            h64 ^= (long) key.get(offset + length - rem + 2) << 16;
        case 2:
            h64 ^= (long) key.get(offset + length - rem + 1) << 8;
        case 1:
            h64 ^= (long) key.get(offset + length - rem);
            h64 *= m64;
        }

        h64 ^= h64 >>> r64;
        h64 *= m64;
        h64 ^= h64 >>> r64;

        return h64;
    }
    
    private static final Unsafe unsafe = JavaInternals.getUnsafe();
    private static final int byteBase =  unsafe.arrayBaseOffset(byte[].class);
    
    public static long hash64u(byte[] key, int offset, int length, long seed)
    {
        long m64 = 0xc6a4a7935bd1e995L;
        int r64 = 47;

        long h64 = (seed & 0xffffffffL) ^ (m64 * length);

        int lenLongs = length >> 3;
            
        long ofs = byteBase + offset;
        
        for (int i = 0; i < lenLongs; ++i)
        {
            long k64 =  unsafe.getLong(key, ofs);
            ofs+=8;

            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }

        int rem = length & 0x7;

        if (rem!=0) {
            h64 = xorBytes(key, rem, h64, rem << 3);
            h64 *= m64;
        }

        h64 ^= h64 >>> r64;
            h64 *= m64;
            h64 ^= h64 >>> r64;

            return h64;
    }

    private static final int charBase =  unsafe.arrayBaseOffset(char[].class);
    private static final int charScale =  unsafe.arrayIndexScale(char[].class);

    private static final long stringValueFieldOffset =  JavaInternals.fieldOffset(String.class, "value");

    public static long hash64u(String key, long seed)
    {
        return hash64u(key, ByteBufferUtil.EMPTY_BYTES, seed);
    }

    public static long hash64u(String key1,byte[] key2, long seed)
    {
        return hash64u( (char[]) unsafe.getObject(key1, stringValueFieldOffset), key2, seed);
    }
    
    public static long hash64u(char[] key1, byte[] key2, long seed)
    {
        long m64 = 0xc6a4a7935bd1e995L;
        int r64 = 47;
        final int length1 = key1.length * charScale;
        int length2 = key2.length;

        long h64 = (seed & 0xffffffffL) ^ (m64 * (length1 + length2) );

        int lenLongs = length1 >> 3;
            
        long ofs = charBase;
        
        long k64;

        for (int i = 0; i < lenLongs; ++i)
        {
            k64 = unsafe.getLong(key1, ofs);
            // fixing wrong byte order of generated char
            k64 = ( (k64 << 8) & 0xFF00FF00FF00FF00l ) | ( ( k64 >> 8 ) & 0x00FF00FF00FF00FFl  )  ;
            ofs+=8;

            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }

        int rem = length1 & 0x7;
        if (rem != 0) {
            if (rem+length2 > 7) {
                // combining long from first and then second arrays
                k64 = orBytes(key2, byteBase, 8-rem, 0);
                k64 = ( (k64 << 8) & 0xFF00FF00FF00FF00l ) | ( ( k64 >> 8 ) & 0x00FF00FF00FF00FFl  )  ;
                k64 = orBytes(key1, ofs, rem, k64);
                // fixing wrong byte order of generated char
                k64 = ( (k64 << 8) & 0xFF00FF00FF00FF00l ) | ( ( k64 >> 8 ) & 0x00FF00FF00FF00FFl  )  ;
                rem = 8 - rem;
                
                k64 *= m64;
                k64 ^= k64 >>> r64;
                k64 *= m64;

                h64 ^= k64;
                h64 *= m64;

            } else {
                // last long
                h64 = xorBytes(key2, length2, h64, (rem + length2 ) << 3);
                h64 = xorChars(key1, rem, h64, rem  << 3);

                h64 *= m64;
                
                h64 ^= h64 >>> r64;
                h64 *= m64;
                h64 ^= h64 >>> r64;

                return h64;
            }
        }

        ofs = byteBase + rem;
        length2-=rem;
        lenLongs = length2 >> 3;

        for (int i = 0; i < lenLongs; ++i)
        {
            k64 = unsafe.getLong(key2, ofs);
            ofs+=8;

            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }
            
        rem = length2 & 0x7;
        if (rem!=0) {
            h64 = xorBytes(key2, rem, h64, rem << 3);
            h64 *= m64;
        }

        h64 ^= h64 >>> r64;
        h64 *= m64;
        h64 ^= h64 >>> r64;

        return h64;
    }
    
    private static long orBytes(Object o, long ofs, int rem, long r) {
        
        ofs+=rem;
        while (rem-->0) {
            r = ( r << 8 ) | ( unsafe.getByte(o,--ofs) & 0xFFl );
        }
        
        return r;
    }
    
    private static long xorBytes(byte[] o, int rem, long r, int shift) {
        
        int ofs= o.length ;
        
        while (rem-->0)
        {
            shift -= 8;
            r ^= (long) o[--ofs] << shift;
        }
        
        return r;
    }
    
    private static long xorChars(char[] o, int rem, long r, int shift) {

        int i = o.length << 1;
        while (rem -->0) {
            char c = o[ --i >> 1 ];
            long b = (i & 1) == 0 ? (byte) ( (c & 0xFF00) >>8) : (byte) (c & 0xFF);
            shift -= 8;
            
            r ^= b << shift;
        }
        
        return r;
    }
    
    public static ByteBuffer toByteBuffer(String s, ByteBuffer byteBuffer)
    {
        int strLen=s.length()*2;
        
        for (int i=s.length(),j=strLen;i-->0;)
        {
            char c = s.charAt(i);
            
            byte b1 = (byte) (c & 0xFF);
            byteBuffer.put(--j,b1);
            byte b2 = (byte) ( (c & 0xFF00) >>8);
            byteBuffer.put(--j,b2);
            
        }
        
        return byteBuffer;
    }

    public static void main(String[] args) {
        Random random = new Random();
        String das = "а роза лежала на шапке азора, lazy dog jumps over the greedy fox";
//        String das = "0123456789012345";//6789012345678901234567890123456789";
        int i;
        for (i=0;i<100000;i++) {
            int l;
            l =  random.nextInt(2560);
            byte[] b = new byte[l];
            if (l>0)
                random.nextBytes(b);
//            byte b[] = { (byte)-1 };
            long l1 = MurmurHash.hash64(ByteBuffer.wrap(b), 0, b.length, 0);
            long l2 = MurmurHash.hash64u("",b, 0);
            if (l1!=l2) {
                System.out.println(String.format("Fuckshit %s!=%s",l1,l2));
            }
            l = random.nextInt(das.length());
            String s = das.substring(l);
            ByteBuffer bb = ByteBuffer.allocate(s.length()*2 + b.length);
            toByteBuffer(s, bb);
            bb.position( s.length() * 2);
            bb.put(b);
            bb.limit(bb.position());
            bb.position(0);
            l1 = MurmurHash.hash64(bb, bb.position(), bb.remaining(), 0);
            l2 = MurmurHash.hash64u(s,b, 0);
            if (l1!=l2) {
                System.out.println(String.format("%s: Fuckstring %s!=%s. strlen = %s, bytelen = %s",i,l1,l2,s.length(), Integer.toHexString( b.length )));
            }
        }
        System.out.println(String.format("i=%s",i));
    }

}

