package utils;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import java.lang.StringBuilder;

public class TPCH7OutputCollector {
	OutputCollector<Text,FloatWritable> base_collector;

	public TPCH7OutputCollector(
			OutputCollector<Text,FloatWritable> base_collector
	) {
		this.base_collector = base_collector;
	}

	public void collect(
			String supp_nation, 
			String cust_nation,
			int year,
			float revenue
	) throws java.io.IOException {
		StringBuilder b = new StringBuilder();
		b.append(supp_nation);
		b.append('|');
		b.append(cust_nation);
		b.append('|');
		b.append(year);
		base_collector.collect(new Text(b.toString()), 
				new FloatWritable(revenue));
	}
}