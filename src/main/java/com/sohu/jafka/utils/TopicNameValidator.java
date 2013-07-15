/**
 *
 */
package com.sohu.jafka.utils;

import java.util.regex.Pattern;

/**
 * Topic name validator
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.3
 */
public class TopicNameValidator {

    private static final String illegalChars = "/" + '\u0000' + '\u0001' + "-" + '\u001F' + '\u007F' + "-" + '\u009F' + '\uD800' + "-" + '\uF8FF' + '\uFFF0'
            + "-" + '\uFFFF';
    private static final Pattern p = Pattern.compile("(^\\.{1,2}$)|[" + illegalChars + "]");

    public static void validate(String topic) {
        if (topic.length() == 0) {
            throw new IllegalArgumentException("topic name is emtpy");
        }
        if (topic.length() > 255) {
            throw new IllegalArgumentException("topic name is too long");
        }
        if (p.matcher(topic).find()) {
            throw new IllegalArgumentException("topic name [" + topic + "] is illegal");
        }
    }

}
