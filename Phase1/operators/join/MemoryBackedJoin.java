package operators.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import operators.projection.ProjectionFilter;
import operators.selection.SelectionFilter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import relations.Relation;
import relations.Schema;

public class MemoryBackedJoin {

	/**
	 * Outer is the smaller relation, i.e. it's values per key are copied into mem
	 * in the reduce phase
	 * 
	 */

	public static final String PARAM_OUTER_FILE_PATH = "outer_file_path";
	
	private static final boolean IS_LOCAL = false;
	//private static final boolean DEBUG = true;

	/** Mapper class */
	public static class JoinMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {

		protected int joinCol;
		protected SelectionFilter selectionFilter;
		protected ProjectionFilter projectionFilter;

		protected HashMap <String, List<String>> outerHashMap; 

		@Override
		public void configure(JobConf conf) {
			// Configuring for outer relation
			joinCol = conf.getInt(HadoopJoin.PARAM_OUTER_JOIN_COL, 0);
			selectionFilter = new SelectionFilter(conf, HadoopJoin.PREFIX_JOIN_OUTER);
			projectionFilter = new ProjectionFilter(conf, HadoopJoin.PREFIX_JOIN_OUTER);

			outerHashMap = new HashMap<String , List<String>>();

			try {			
				FileSystem fs = FileSystem.get(conf);
				Path outerFilePath = new Path(conf.get(PARAM_OUTER_FILE_PATH));
				FileStatus[] files = fs.listStatus(outerFilePath);
				
				for (FileStatus file : files) {
					if (!file.isDir()) {
						BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
						
						String line = null;
						while((line = br.readLine()) != null) {				
							String tuple[] = line.split(Schema.COLUMN_SEPARATOR_RE);
	
							// selection
							if (!selectionFilter.checkSelection(tuple))
								continue;

							// projection
							tuple = projectionFilter.projection(tuple, joinCol);

							StringBuffer attrs = new StringBuffer();
							for (int i = 1; i < tuple.length; i++) {
								attrs.append(tuple[i] + Schema.COLUMN_SEPARATOR);
							}

							if (attrs.length() > 0) {
								attrs.deleteCharAt(attrs.length() - 1);
							}

							// insert into Hashtable
							if(outerHashMap.containsKey(tuple[0])) {
								outerHashMap.get(tuple[0]).add(attrs.toString());
							} else {
								List<String> values = new ArrayList<String>();
								values.add(attrs.toString());
								outerHashMap.put(tuple[0], values);
							}
							
						}
					}
				}
			} catch(Exception e) {
				e. printStackTrace ();
			}

			// Re-configuring for inner relation
			joinCol = conf.getInt(HadoopJoin.PARAM_INNER_JOIN_COL, 0);
			selectionFilter = new SelectionFilter(conf, HadoopJoin.PREFIX_JOIN_INNER);
			projectionFilter = new ProjectionFilter(conf, HadoopJoin.PREFIX_JOIN_INNER);
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			String[] tuple = value.toString().split(Schema.COLUMN_SEPARATOR_RE);

			// selection
			if (!selectionFilter.checkSelection(tuple))
				return;

			// projection
			tuple = projectionFilter.projection(tuple, joinCol);

			// extract fields
			StringBuffer attrs = new StringBuffer();
			for (int i = 1; i < tuple.length; i++) {
				attrs.append(tuple[i] + Schema.COLUMN_SEPARATOR);
			}

			if (attrs.length() > 0) {
				attrs.deleteCharAt(attrs.length() - 1);
			}

			// join
			if (outerHashMap.containsKey(tuple[0])) {
				for(String outerTuple : outerHashMap.get(tuple[0])) {
					output.collect(new Text(tuple[0]), new Text(outerTuple + Schema.COLUMN_SEPARATOR + attrs));
				}
			}
		}
	}

	/** Reducer class */
	public static class JoinReducer extends MapReduceBase implements
		Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

			while (values.hasNext()) {
				output.collect(null, values.next());
			}
		}
	}

	/** join configuration */
	public static JobConf createJob(Configuration conf_, Relation outer, String outerJoinCol, 
			Relation inner, String innerJoinCol, Relation outRelation) throws IOException {

		if (IS_LOCAL) {
			conf_.set("mapred.job.tracker", "local");
			conf_.set("fs.default.name", "file://///");
		}

		// Set join columns
		conf_.setInt(HadoopJoin.PARAM_OUTER_JOIN_COL, 
				outer.schema.getColumnIndex(outerJoinCol));
		conf_.setInt(HadoopJoin.PARAM_INNER_JOIN_COL, 
				inner.schema.getColumnIndex(innerJoinCol));
		
		conf_.set(PARAM_OUTER_FILE_PATH, outer.storageFileName);

		JobConf conf = new JobConf(conf_);
		conf.setJobName("M_" + outer.name + "|><|" + inner.name);

		conf.setJarByClass(ReduceSideJoin.class);

		// Input & Output path
		FileInputFormat.setInputPaths(conf, new Path(inner.storageFileName));
		FileOutputFormat.setOutputPath(conf, new Path(outRelation.storageFileName));
		
		// Mapper classes
		conf.setMapperClass(JoinMapper.class);

		// Mapper output class
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		// Reducer
		conf.setReducerClass(JoinReducer.class);

		// Reducer output
		// TODO: SequenceFileInputFormat may be more efficient way of storing
		// intermediate data!

		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
	}

	/** Convenience method to made definitions shorter: Natural join */
	public static JobConf createJob(Configuration conf, Relation outer, Relation inner,
			String naturalJoinCol, Relation outRelation) throws IOException {
		return createJob(conf, outer, naturalJoinCol, inner, naturalJoinCol, outRelation);
	}
}
