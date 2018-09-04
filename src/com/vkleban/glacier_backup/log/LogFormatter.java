package com.vkleban.glacier_backup.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;


public class LogFormatter extends Formatter {
	
    private static final DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    /**
     * @return Skip all logger caller stack to find the first method form this package. Return its line number
     */
    private int getOurClosestCallerLine(String callerClass, String callerMethod) {
    	StackTraceElement[] stackTrace= Thread.currentThread().getStackTrace();
    	for (int i=7; i< stackTrace.length; i++)
    	{
    		StackTraceElement entry= stackTrace[i];
    		if (entry.getClassName().equals(callerClass) && entry.getMethodName().equals(callerMethod))
    			return entry.getLineNumber();
    	}
    	return -1;
    }

	@Override
	public String format(LogRecord record) {
        ZonedDateTime timestamp= ZonedDateTime.ofInstant(
        		Instant.ofEpochMilli(record.getMillis()),
        		ZoneOffset.systemDefault());
        StringBuilder sb= new StringBuilder(timestamp.format(timestampFormatter));
        sb.append(String.format(" %-8s", record.getLevel().getName()));
        String className= record.getSourceClassName();
        if ( className != null) {
            sb.append(className);
            String methodName= record.getSourceMethodName();
            if ( methodName != null) {
            	sb.append(String.format(
            			"::%s:%d:%s ",
            			methodName,
            			getOurClosestCallerLine(className, methodName),
            			Thread.currentThread().getName()));
            }
        }
        sb.append(formatMessage(record));
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            sb.append(" ").append(sw.toString());
        }
        sb.append("\n");
        return sb.toString();
	}

}
