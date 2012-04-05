package com.sohu.jafka.console;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import joptsimple.HelpFormatter;
import joptsimple.OptionDescriptor;

public class MyFormatter implements HelpFormatter {
        public String format( Map<String, ? extends OptionDescriptor> options ) {
            StringBuilder buffer = new StringBuilder();
            for ( OptionDescriptor each : new HashSet<OptionDescriptor>( options.values() ) ) {
                buffer.append( lineFor( each ) );
            }
            return buffer.toString();
        }

        private String lineFor( OptionDescriptor descriptor ) {
            StringBuilder line = new StringBuilder();
            for(String option:descriptor.options()) {
                line.append(option.length()>1?"--":"-");
                line.append(option);
                line.append(',');
            }
            line.setCharAt(line.length()-1, ' ');
            line.append("       ");
            final int blankSize = line.length() + 2;
            if(descriptor.isRequired()) {
                line.append("REQUIRED: ");
            }
            line.append(descriptor.description());
            List<?> list = descriptor.defaultValues();
            if(list!=null&&list.size()>0) {
                line.append("\n");
                for(int i=0;i<blankSize;i++) {
                    line.append(' ');
                }
                line.append("(default: ");
                for(int i=0;i<list.size();i++) {
                    if(i>0)line.append(',');
                    line.append(list.get(i).toString());
                }
                line.append(')');
            }
            line.append( System.getProperty( "line.separator" ) );
            return line.toString();
        }
    }