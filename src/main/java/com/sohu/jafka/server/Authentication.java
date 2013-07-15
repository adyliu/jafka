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

package com.sohu.jafka.server;

import com.sohu.jafka.utils.Utils;

/**
 * Authentication for some operations
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
@SuppressWarnings("ALL")
public abstract class Authentication {

   public abstract boolean auth(String password);

   public static class PlainAuth extends Authentication {

        protected final String password;

        public PlainAuth(String passwd) {
            this.password = passwd;
        }

        @Override
        public boolean auth(String passwd) {
            return password == null || password.equals(passwd);
        }
    }

   public static class Crc32Auth extends Authentication {

        protected final long password;

        public Crc32Auth(long passwd) {
            this.password = passwd;
        }

        @Override
        public boolean auth(String passwd) {
            return password == Utils.crc32(Utils.getBytes(passwd));
        }
    }

   public static class Md5Auth extends Authentication {

        final String password;

        public Md5Auth(String passwd) {
            this.password = passwd;
        }

        @Override
        public boolean auth(String passwd) {
            return password.equals(Utils.md5(Utils.getBytes(passwd)));
        }
    }
    /**
     * build an Authentication.
     * 
     * Types:
     * <ul>
     * <li>plain:jafka</li>
     * <li>md5:77be29f6d71ec4e310766ddf881ae6a0</li>
     * <li>crc32:1725717671</li>
     * </ul>
     * @param crypt password style
     * @return an authentication 
     * @throws IllegalArgumentException
     */
    public static Authentication build(String crypt) throws IllegalArgumentException {
        if(crypt == null) {
            return new PlainAuth(null);
        }
        String[] value = crypt.split(":");
        
        if(value.length == 2 ) {
            String type = value[0].trim();
            String password = value[1].trim();
            if(password!=null&&password.length()>0) {
                if("plain".equals(type)) {
                    return new PlainAuth(password);
                }
                if("md5".equals(type)) {
                    return new Md5Auth(password);
                }
                if("crc32".equals(type)) {
                    return new Crc32Auth(Long.parseLong(password));
                }
            }
        }
        throw new IllegalArgumentException("error password: "+crypt);
    }
}
