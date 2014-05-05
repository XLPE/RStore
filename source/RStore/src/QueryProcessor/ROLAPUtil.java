package QueryProcessor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ROLAPUtil {
    public static String getTimeStr(long t){
		SimpleDateFormat dt = new SimpleDateFormat("z_yyyy.MM.dd_hh.mm.ss");
		dt.setTimeZone(TimeZone.getTimeZone("PST"));
		String dateStr = dt.format(new Date(t));	
		return dateStr;
    }
    
    public static long getTimestamp(String str) throws ParseException{
    	SimpleDateFormat dt = new SimpleDateFormat("z_yyyy.MM.dd_hh.mm.ss");
		dt.setTimeZone(TimeZone.getTimeZone("PST"));
		return dt.parse(str).getTime();
    }
}
