package operators.join;

import java.io.IOException;
import java.util.*;

import operators.projection.ProjectionFilter;
import operators.selection.SelectionFilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import relations.Relation;
import relations.Schema;

public class ReduceSideJoin {

	/**
	 * Outer is the smaller relation, i.e. it's values per key are copied into 
	 * memory in the reduce phase
	 */

	private static final boolean IS_LOCAL = false;
	//private static final boolean DEBUG = true;

	/** outer relation mapper class*/
	public static class OuterRelationMapper extends ReduceSideJoinAbstractMapper {
		public void configure(JobConf conf) {
			reduceOrder = "0";
			joinCol = conf.getInt(HadoopJoin.PARAM_OUTER_JOIN_COL, 0);
			
			selectionFilter = new SelectionFilter(conf, HadoopJoin.PREFIX_JOIN_OUTER);
			projectionFilter = new ProjectionFilter(conf, HadoopJoin.PREFIX_JOIN_OUTER);
		};
	}

	/** inner relation mapper class*/
	public static class InnerRelationMapper extends ReduceSideJoinAbstractMapper {
		public void configure(JobConf conf) {
			reduceOrder = "1";
			joinCol = conf.getInt(HadoopJoin.PARAM_INNER_JOIN_COL, 0);

			selectionFilter = new SelectionFilter(conf, HadoopJoin.PREFIX_JOIN_INNER);
			projectionFilter = new ProjectionFilter(conf, HadoopJoin.PREFIX_JOIN_INNER);
		}
	}

	/** mapper class */
	public static class ReduceSideJoinAbstractMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextPair, TextPair> {

		// Overridden by child class
		protected String reduceOrder;
		
		protected int joinCol;

		protected SelectionFilter selectionFilter;
		
		protected ProjectionFilter projectionFilter;

		public void map(LongWritable key, Text value, OutputCollector<TextPair, TextPair> output,
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
			
			output.collect(new TextPair(tuple[0], reduceOrder), new TextPair(attrs.toString(),
						reduceOrder));
		}
	}

	/** partitioner only by the first value of key <TextPair> */
	public static class KeyPartitioner implements Partitioner<TextPair, TextPair> {
		@Override
		public int getPartition(TextPair key, TextPair value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0) {
		}
	}

	/** Reducer class */
	public static class JoinReducer extends MapReduceBase implements
	Reducer<TextPair, TextPair, NullWritable, Text> {

		private static final Text smallerRelmarker = new Text("0");

		public void reduce(TextPair key, Iterator<TextPair> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

			ArrayList<String> outer_relation_tuples = new ArrayList<String>();

			TextPair value = null;
			String result_tuple = null;
			String inner_rel_tuple = null;

			while (values.hasNext()) {
				value = values.next();
				if ((value.getSecond().compareTo(smallerRelmarker) == 0)) {
					// add the whole tuple of smaller relation
					outer_relation_tuples.add(value.getFirst().toString());
				} else {
					// We do INNER join, so if outer relation not present, we can quit
					// immediately!
					if (outer_relation_tuples.size() == 0) {
						return;
					}
					
					inner_rel_tuple = value.getFirst().toString();
					for (String outer_rel_tuple : outer_relation_tuples) {
						result_tuple = outer_rel_tuple + Schema.COLUMN_SEPARATOR + inner_rel_tuple;
						
						// Instead of key.getFirst() we output null for a key, so only the
						// tuple gets written into intermediate file
						output.collect(null, new Text(result_tuple));
					}
				}
			}
		}
	}

	/** join configuration */
	public static JobConf createJob(Configuration conf_, Relation outer, String outerJoinCol, 
			Relation inner, String innerJoinCol, Relation outRelation) throws IOException {

		if (IS_LOCAL) {
			conf_.set("mapred.job.tracker", "local");
			conf_.set("fs.default.name", "file:///");
		}

		// Set ReduceSideJoin columns
		conf_.setInt(HadoopJoin.PARAM_OUTER_JOIN_COL, outer.schema.getColumnIndex(outerJoinCol));
		conf_.setInt(HadoopJoin.PARAM_INNER_JOIN_COL, inner.schema.getColumnIndex(innerJoinCol));

		JobConf conf = new JobConf(conf_);
		conf.setJobName("R_" + outer.name + "|><|" + inner.name);

		conf.setJarByClass(ReduceSideJoin.class);

		// Mapper classes & Input files
		MultipleInputs.addInputPath(conf, new Path(outer.storageFileName), TextInputFormat.class,
				OuterRelationMapper.class);
		MultipleInputs.addInputPath(conf, new Path(inner.storageFileName), TextInputFormat.class,
				InnerRelationMapper.class);

		// Output path
		FileOutputFormat.setOutputPath(conf, new Path(outRelation.storageFileName));

		// Mapper output class
		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(TextPair.class);

		// Mapper Value Grouping
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);

		// Partitioner
		conf.setPartitionerClass(KeyPartitioner.class);

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
