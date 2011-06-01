/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.Random;

public class BKeyGenerator {
    private static ByteBuffer randomKey(Random r) {
        byte[] bytes = new byte[48];
        r.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    static class RandomStringGenerator implements ResetableIterator<ByteBuffer> {
        int i, n, seed;
        Random random;

        RandomStringGenerator(int seed, int n) {
            i = 0;
            this.seed = seed;
            this.n = n;
            reset();
        }

        public int size() {
            return n;
        }

        public void reset() {
            random = new Random(seed);
        }

        public boolean hasNext() {
            return i < n;
        }

        public ByteBuffer next() {
            i++;
            return randomKey(random);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
