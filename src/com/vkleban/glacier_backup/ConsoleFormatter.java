package com.vkleban.glacier_backup;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class ConsoleFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
        StringBuilder sb= new StringBuilder();
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
