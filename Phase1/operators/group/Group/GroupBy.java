package operators.group.Group;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

import operators.group.*;

public class GroupBy extends Configured implements Tool {

	protected static final String COLUMN_SEPARATOR_RE = " ";
	protected static final String COLUMN_SEPARATOR = " ";

	private static boolean IS_LOCAL = false;
	private static final boolean DEBUG = true;

	public static class GroupByMapper extends MapReduceBase implements Mapper<LongWritable, Text, TextTriple, Text> {

		// TODO: Overridden by child class
		protected static String reduceOrder;
		protected static int groupbycol1 = 0;
		protected static int groupbycol2 = 1;
		protected static int groupbycol3 = 2;

		@Override
		public void configure(JobConf conf) {
			groupbycol1 = conf.getInt("groupbyCol1", 0);
			groupbycol2 = conf.getInt("groupbyCol2", 0);
			groupbycol3 = conf.getInt("groupbyCol3", 0);
		}

		public void map(LongWritable key, Text value, OutputCollector<TextTriple, Text> output, Reporter reporter) throws IOException {
			String[] tuple = value.toString().split(COLUMN_SEPARATOR_RE);

			if (DEBUG)
				System.out.println("sel ok: " + value);

			StringBuffer attrs = new StringBuffer();
			for (int i = 0; i < tuple.length; i++) {
				attrs.append(tuple[i] + COLUMN_SEPARATOR);
			}

			if (attrs.length() > 0) {
				attrs.deleteCharAt(attrs.length() - 1);
			}

			output.collect(new TextTriple(tuple[groupbycol1], tuple[groupbycol2], tuple[groupbycol3]), new Text(attrs.toString()));
		}

	}

	public static class GroupByReducer extends MapReduceBase implements Reducer<TextTriple, Text, TextTriple, Text> {
		protected static int aggregationCol = 2;

		@Override
		public void configure(JobConf conf) {
			aggregationCol = conf.getInt("aggregationCol", 0);
		}

		public void reduce(TextTriple key, Iterator<Text> values, OutputCollector<TextTriple, Text> output, Reporter reporter) throws IOException {
			ArrayList<String> buffer = new ArrayList<String>();

			Text value = null;
			String bAttrs = null;

			int aggregatedVal = 0;
			while (values.hasNext()) {
				value = values.next();
				String[] tuple = value.toString().split(COLUMN_SEPARATOR_RE);
				aggregatedVal += Integer.parseInt(tuple[aggregationCol]);
			}
			StringBuffer attrs = new StringBuffer();
			attrs.append(key.getFirst() + COLUMN_SEPARATOR);
			attrs.append(key.getSecond() + COLUMN_SEPARATOR);
			attrs.append(key.getThird() + COLUMN_SEPARATOR);
			attrs.append(aggregatedVal);
			
			output.collect(key, new Text(attrs.toString()));

		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 6) {
			System.out.println("USAGE: <prog name> <input> <groupbycol> <groupbycol> <groupbycol> <aggregationcol> <output>");
			return -1;
		}

		String inRelation = args[0];
		int groupbyCol1 = Integer.parseInt(args[1]);
		int groupbyCol2 = Integer.parseInt(args[2]);
		int groupbyCol3 = Integer.parseInt(args[3]);

		int aggregationCol = Integer.parseInt(args[4]);

		String output = args[5];

		JobConf conf = getGroupByConf(getConf(), inRelation, groupbyCol1, groupbyCol2, groupbyCol3, aggregationCol, output);

		// Run job
		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * Returns default configuration
	 * 
	 * @param
	 * @return
	 */
	public static JobConf getGroupByConf(Configuration conf_, String inPath, int groupbyCol1, int groupbyCol2, int groupbyCol3, int aggregationCol, String outputPath) {
		JobConf conf = new JobConf(conf_, GroupBy.class);

		if (IS_LOCAL) {
			conf.set("mapred.job.tracker", "local");
			conf.set("fs.default.name", "file:///");
		}

		conf.setInt("groupbyCol1", groupbyCol1);
		conf.setInt("groupbyCol2", groupbyCol2);
		conf.setInt("groupbyCol3", groupbyCol3);
		conf.setInt("aggregationCol", aggregationCol);

		// Mapper output class
		conf.setMapOutputKeyClass(TextTriple.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(GroupByMapper.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Input Path
		FileInputFormat.setInputPaths(conf, new Path(inPath));

		// Output path
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		// Reducer
		conf.setReducerClass(GroupByReducer.class);

		return conf;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GroupBy(), args);
		System.exit(res);
	}
}
