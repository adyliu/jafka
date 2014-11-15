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

import com.sohu.jafka.mx.IMBeanName;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.*;
import java.util.zip.CRC32;

/**
 * common utilities
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Utils {
    /**
     * loading Properties from files
     *
     * @param filename file path
     * @return properties
     * @throws RuntimeException while file not exist or loading fail
     */
    public static Properties loadProps(String filename) {
        Properties props = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(filename);
            props.load(fis);
            return props;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(fis);
        }

    }

    /**
     * Get a property of type java.util.Properties or return the default if
     * no such property is defined
     */
    public static Properties getProps(Properties props, String name, Properties defaultProperties) {
        final String propString = props.getProperty(name);
        if (propString == null) return defaultProperties;
        String[] propValues = propString.split(",");
        if (propValues.length < 1) {
            throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'");
        }
        Properties properties = new Properties();
        for (int i = 0; i < propValues.length; i++) {
            String[] prop = propValues[i].split("=");
            if (prop.length != 2) throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues[i] + "'");
            properties.put(prop[0], prop[1]);
        }
        return properties;
    }

    /**
     * Get a string property, or, if no such property is defined, return
     * the given default value
     *
     * @param props        the properties
     * @param name         the key in the properties
     * @param defaultValue the default value if the key not exists
     * @return value in the props or defaultValue while name not exist
     */
    public static String getString(Properties props, String name, String defaultValue) {
        return props.containsKey(name) ? props.getProperty(name) : defaultValue;
    }

    public static String getString(Properties props, String name) {
        if (props.containsKey(name)) {
            return props.getProperty(name);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    public static int getInt(Properties props, String name) {
        if (props.containsKey(name)) {
            return getInt(props, name, -1);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    public static int getInt(Properties props, String name, int defaultValue) {
        return getIntInRange(props, name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public static int getIntInRange(Properties props, String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.getProperty(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }
    public static int getIntInRange(Map<String,String> props, String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.get(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }

    public static boolean getBoolean(Properties props, String name, boolean defaultValue) {
        if (!props.containsKey(name)) return defaultValue;
        return "true".equalsIgnoreCase(props.getProperty(name));
    }

    private static Map<String, Integer> getCSVMap(String value, String exceptionMsg, String successMsg) {
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        if (value == null || value.trim().length() < 3) return map;
        for (String one : value.trim().split(",")) {
            String[] kv = one.split(":");
            //FIXME: force positive number
            map.put(kv[0].trim(), Integer.valueOf(kv[1].trim()));
        }
        return map;
    }

    public static Map<String, Integer> getTopicRentionHours(String retentionHours) {
        String exceptionMsg = "Malformed token for topic.log.retention.hours in server.properties: ";
        String successMsg = "The retention hour for ";
        return getCSVMap(retentionHours, exceptionMsg, successMsg);
    }

    public static Map<String, Integer> getTopicFlushIntervals(String allIntervals) {
        String exceptionMsg = "Malformed token for topic.flush.Intervals.ms in server.properties: ";
        String successMsg = "The flush interval for ";
        return getCSVMap(allIntervals, exceptionMsg, successMsg);
    }

    public static KV<String, Integer> getTopicPartition(String topicPartition) {
        int index = topicPartition.lastIndexOf('-');
        return new KV<String, Integer>(topicPartition.substring(0, index),//
                Integer.valueOf(topicPartition.substring(index + 1)));
    }

    public static Map<String, Integer> getTopicPartitions(String allPartitions) {
        String exceptionMsg = "Malformed token for topic.partition.counts in server.properties: ";
        String successMsg = "The number of partitions for topic  ";
        return getCSVMap(allPartitions, exceptionMsg, successMsg);
    }

    public static Map<String, Integer> getConsumerTopicMap(String consumerTopicString) {
        String exceptionMsg = "Malformed token for embeddedconsumer.topics in consumer.properties: ";
        String successMsg = "The number of consumer thread for topic  ";
        return getCSVMap(consumerTopicString, exceptionMsg, successMsg);
    }

    /**
     * read data from channel to buffer
     *
     * @param channel readable channel
     * @param buffer  bytebuffer
     * @return read size
     * @throws IOException
     */
    public static int read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        int count = channel.read(buffer);
        if (count == -1) throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");
        return count;
    }

    /**
     * Write a size prefixed string where the size is stored as a 2 byte
     * short
     *
     * @param buffer The buffer to write to
     * @param s      The string to write
     */
    public static void writeShortString(ByteBuffer buffer, String s) {
        if (s == null) {
            buffer.putShort((short) -1);
        } else if (s.length() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
        } else {
            byte[] data = getBytes(s); //topic support non-ascii character
            buffer.putShort((short) data.length);
            buffer.put(data);
        }
    }

    public static String fromBytes(byte[] b) {
        return fromBytes(b, "UTF-8");
    }

    public static String fromBytes(byte[] b, String encoding) {
        if (b == null) return null;
        try {
            return new String(b, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b);
        }
    }

    public static byte[] getBytes(String s) {
        return getBytes(s, "UTF-8");
    }

    public static byte[] getBytes(String s, String encoding) {
        if (s == null) return null;
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
    }

    /**
     * Read an unsigned integer from the current position in the buffer,
     * incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long getUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying
     * the buffers position
     *
     * @param buffer The buffer to read from
     * @param index  the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long getUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is
     * ignored.
     *
     * @param buffer The buffer to write to
     * @param value  The value to write
     */
    public static void putUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is
     * ignored.
     *
     * @param buffer The buffer to write to
     * @param index  The position in the buffer at which to begin writing
     * @param value  The value to write
     */
    public static void putUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Compute the CRC32 of the byte array
     *
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public static long crc32(byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * Compute the CRC32 of the segment of the byte array given by the
     * specificed size and offset
     *
     * @param bytes  The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size   the number of bytes to checksum
     * @return The CRC32
     */
    public static long crc32(byte[] bytes, int offset, int size) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Create a new thread
     *
     * @param name     The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon   Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        return thread;
    }

    /**
     * read bytes with a short sign prefix(mark the size of bytes)
     *
     * @param buffer data buffer
     * @return string result(encoding with UTF-8)
     * @see #writeShortString(ByteBuffer, String)
     */
    public static String readShortString(ByteBuffer buffer) {
        short size = buffer.getShort();
        if (size < 0) {
            return null;
        }
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return fromBytes(bytes);
    }

    /**
     * caculate string length with size prefix
     *
     * @param topic the string value (support UTF-8 bytes)
     * @return string size with short prefix (2+topic.length)
     * @see #readShortString(ByteBuffer)
     */
    public static int caculateShortString(String topic) {
        return 2 + getBytes(topic).length;
    }

    public static boolean registerMBean(IMBeanName object) {
        return registerMBean(object, object.getMbeanName());
    }

    /**
     * Register the given mbean with the platform mbean server,
     * unregistering any mbean that was there before. Note, this method
     * will not throw an exception if the registration fails (since there
     * is nothing you can do and it isn't fatal), instead it just returns
     * false indicating the registration failed.
     *
     * @param mbean The object to register as an mbean
     * @param name  The name to register this mbean with
     * @return true if the registration succeeded
     */
    static boolean registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName)) {
                    mbs.unregisterMBean(objName);
                }
                mbs.registerMBean(mbean, objName);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void unregisterMBean(IMBeanName mbean) {
        unregisterMBean(mbean.getMbeanName());
    }

    /**
     * Unregister the mbean with the given name, if there is one registered
     *
     * @param name The mbean name to unregister
     * @see #registerMBean(Object, String)
     */
    private static void unregisterMBean(String name) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName)) {
                    mbs.unregisterMBean(objName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * open a readable or writeable FileChannel
     *
     * @param file    file object
     * @param mutable writeable
     * @return open the FileChannel
     */
    @SuppressWarnings("resource")
    public static FileChannel openChannel(File file, boolean mutable) throws IOException {
        if (mutable) {
            return new RandomAccessFile(file, "rw").getChannel();
        }
        return new FileInputStream(file).getChannel();
    }

    public static List<String> getCSVList(String csvList) {
        if (csvList == null || csvList.length() == 0) return Collections.emptyList();
        List<String> ret = new ArrayList<String>(Arrays.asList(csvList.split(",")));
        Iterator<String> iter = ret.iterator();
        while (iter.hasNext()) {
            final String next = iter.next();
            if (next == null || next.length() == 0) {
                iter.remove();
            }
        }
        return ret;
    }

    /**
     * create an instance from the className
     *
     * @param className full class name
     * @return an object or null if className is null
     */
    @SuppressWarnings("unchecked")
    public static <E> E getObject(String className) {
        if (className == null) {
            return (E) null;
        }
        try {
            return (E) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toString(ByteBuffer buffer, String encoding) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return fromBytes(bytes, encoding);
    }

    public static File getCanonicalFile(File f) {
        try {
            return f.getCanonicalFile();
        } catch (IOException e) {
            return f.getAbsoluteFile();
        }
    }

    public static ByteBuffer serializeArray(long[] numbers) {
        int size = 4 + 8 * numbers.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(numbers.length);
        for (long num : numbers) {
            buffer.putLong(num);
        }
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer serializeArray(int[] numbers) {
        int size = 4 + 4 * numbers.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(numbers.length);
        for (int num : numbers) {
            buffer.putInt(num);
        }
        buffer.rewind();
        return buffer;
    }

    public static int[] deserializeIntArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        int[] nums = new int[size];
        for (int i = 0; i < size; i++) {
            nums[i] = buffer.getInt();
        }
        return nums;
    }

    public static long[] deserializeLongArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        long[] nums = new long[size];
        for (int i = 0; i < size; i++) {
            nums[i] = buffer.getLong();
        }
        return nums;
    }

    private final static char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * digest message with MD5
     *
     * @param source message
     * @return 32 bit MD5 value (lower case)
     */
    public static String md5(byte[] source) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(source);
            byte tmp[] = md.digest();
            char str[] = new char[32];
            int k = 0;
            for (byte b : tmp) {
                str[k++] = hexDigits[b >>> 4 & 0xf];
                str[k++] = hexDigits[b & 0xf];
            }
            return new String(str);

        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        if (dir.isDirectory()) {
            File[] subs = dir.listFiles();
            if (subs != null) {
                for (File f : subs) {
                    deleteDirectory(f);
                }
            }
        }
        if (!dir.delete()) {
            throw new IllegalStateException("delete directory failed: " + dir);
        }
    }
}
