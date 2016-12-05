package Houses;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.text.DecimalFormat;


public class HousesReducer  extends Reducer <Text,IntWritable,Text,FloatWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// TODO: initialize min and max values
		   String neighbourhood=key.toString();
                   int count=0;
		   float mean=0.0f,min=Float.MAX_VALUE,max=0.0f,value=0.0f,sum=0.0f;
		// TODO: loop through values to determine min, max, count, and sum
		  while(values.iterator().hasNext())
		   {
		 
		     IntWritable i=values.iterator().next();
		     value=i.get();
		     
			if(value>=max)
				max=value;
			if(value<=min)
				min=value;
				
		     
			count++;
			sum=sum+value;
			
		   }
	
		// TODO: calculate mean
		   mean=sum/count;

		// TODO: write (key, min) to context
		context.write(new Text(neighbourhood),new FloatWritable(min));
		// TODO: write (key, mean) to context
		context.write(new Text(neighbourhood),new FloatWritable(mean));
		// TODO: write (key, max) to context
		context.write(new Text(neighbourhood),new FloatWritable(max));
   	}
}

