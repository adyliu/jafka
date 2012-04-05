/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sohu.jafka.utils;

/**
 * two elements tuple
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-4-9
 */
public class KV<K, V> {

    public final K k;

    public final V v;

    public KV(K k, V v) {
        super();
        this.k = k;
        this.v = v;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((k == null) ? 0 : k.hashCode());
        result = prime * result + ((v == null) ? 0 : v.hashCode());
        return result;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        KV other = (KV) obj;
        if (k == null) {
            if (other.k != null) return false;
        } else if (!k.equals(other.k)) return false;
        if (v == null) {
            if (other.v != null) return false;
        } else if (!v.equals(other.v)) return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("KV [k=%s, v=%s]", k, v);
    }

    public static class StringKV extends KV<String, String> {

        public StringKV(String k, String v) {
            super(k, v);
        }

    }
}
