/**
 * 
 */
package com.sohu.jafka.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 
 * @author adyliu(imxylz@gmail.com)
 * @since 2012-11-20
 */
public class TopicNameValidatorTest {

    /**
     * Test method for {@link com.sohu.jafka.utils.TopicNameValidator#validate(java.lang.String)}.
     */
    @Test
    public void testValidate() {
        final char[] badchars = {'/', '\u0000', '\u0001', '\u0018', '\u001F', '\u008F', '\uD805', '\uFFFA'};
        for(char c:badchars) {
            final String badTopicName = "bad"+c+"topicname";
            try {
                TopicNameValidator.validate(badTopicName);
                fail("topic name is illegal");
            } catch (IllegalArgumentException e) {
                //ignore
            }
        }
    }

}
