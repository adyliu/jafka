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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Pool<K extends Comparable<K>, V> implements Map<K, V> {

    private final ConcurrentMap<K, V> pool = new ConcurrentSkipListMap<K, V>();

    public int size() {
        return pool.size();
    }

    public boolean isEmpty() {
        return pool.isEmpty();
    }

    public boolean containsKey(Object key) {
        return pool.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return pool.containsValue(value);
    }

    public V get(Object key) {
        return pool.get(key);
    }

    public V put(K key, V value) {
        return pool.put(key, value);
    }

    public V putIfNotExists(K key,V value) {
        return pool.putIfAbsent(key, value);
    }
    public V remove(Object key) {
        return pool.remove(key);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        pool.putAll(m);
    }

    public void clear() {
        pool.clear();
    }

    public Set<K> keySet() {
        return pool.keySet();
    }

    public Collection<V> values() {
        return pool.values();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return pool.entrySet();
    }
    
    @Override
    public String toString() {
        return pool.toString();
    }

}
