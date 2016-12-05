package Houses;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



import java.io.IOException;
import java.util.StringTokenizer;

public class HousesMapper  extends Mapper <LongWritable,Text,Text,IntWritable> {
	private static Log log = LogFactory.getLog(HousesMapper.class);

   	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: skip  very first record (schema line)
		int price=0;
		String neighbourhood="";
		
      		// TODO: create iterator over record assuming comma-separated fields
      		String[]  parts=value.toString().split(",");
                if(parts[0].matches("\\d+"))
		{
		
      		// TODO validate number of tokens in iterator 
		// TODO if invalid, then write a message to log
		if(parts.length==81)
		{
		}
		else
		{
			log.info("Not adequate number of data field values in the input record.");
			return;
		}

		// TODO get neighborhodd
		// TODO validate string is not empty or null
		// TODO if empty or null, then write a message to log 

		// TODO get price
		// TODO convert price to int
		neighbourhood=parts[12];
		if(neighbourhood==null||neighbourhood.isEmpty())
		{
			log.error("neighbourhood not available for this data");
			return;
		}
	        if(parts[80]!=null&&parts[80].matches("\\d+"))
		{
		        price=Integer.parseInt(parts[80]);
		}
		else
		{
			log.error("price is null or is not entirely numeric");
			return;
		}


      		// TODO validate the price is greater than zero 
		// TODO if price <= 0, then write a message to log
	        if(price>0)
		{
		}
		else
		{
			log.error("Price wrongly found to be zero or negative");
			return;
		}

      		// TODO emit key-value as (neighborhood, price) 
      	       context.write(new Text(neighbourhood),new IntWritable(price));

        	}      	
      
   	}

}
