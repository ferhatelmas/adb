package operators.groupby;

import java.io.IOException;
import java.util.*;

import operators.projection.ProjectionFilter;
import operators.selection.SelectionEntry;
import operators.selection.SelectionFilter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import relations.Relation;
import relations.Schema;
import utils.TPCH7OutputCollector;

public class GroupBy {

	/** parameters passed to Mapred job */
	public static final String PARAM_SELECTION = "selection_args";
	public static final String PARAM_PROJECTION = "projection_args";
	public static final String PARAM_GROUPBY = "groupby_args";
	public static final String PARAM_ORDER_LIST = "index_list";
	
	private static final boolean IS_LOCAL = false;
	//private static final boolean DEBUG = true;


	/** @return group key as TextGroup from tuple */
	public static TextGroup group(String[] tuple, String[] groupByFields) {
		ArrayList<String> group = new ArrayList<String>();
		for (String index : groupByFields) {
			group.add(tuple[Integer.parseInt(index)]);
		}
		
		return new TextGroup(group);
	}
	
	/** GroupBy mapper class */
	public static class GroupByMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, TextGroup, FloatWritable> {

		protected int joinCol;
		protected SelectionFilter selectionFilter;
		protected ProjectionFilter projectionFilter;
		protected String[] groupByFields;
		protected List<Integer> indexList;

		protected HashMap <String, List<String>> outerHashMap; 

		@Override
		public void configure(JobConf conf) {
			selectionFilter = new SelectionFilter(conf, PARAM_SELECTION);
			projectionFilter = new ProjectionFilter(conf, PARAM_PROJECTION);
			groupByFields = conf.getStrings(PARAM_GROUPBY);
			String[] sIndexList = conf.getStrings(PARAM_ORDER_LIST);
			indexList = new ArrayList<Integer>();
			for (String index : sIndexList) {
				indexList.add(Integer.parseInt(index));
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<TextGroup, FloatWritable> output,
				Reporter reporter) throws IOException {
			String[] tuple = value.toString().split(Schema.COLUMN_SEPARATOR_RE);
			
			// selection
			if (!selectionFilter.checkSelection(tuple))
				return;

			// projection
			//tuple = projectionFilter.projection(tuple);
			
			// TODO: It's still hard code here
			tuple[indexList.get(0)] = tuple[indexList.get(0)].substring(0, 4);
			
			FloatWritable volume = new FloatWritable(Float.parseFloat(tuple[indexList.get(1)])
					* (1 - Float.parseFloat(tuple[indexList.get(2)])));
			
			TextGroup tg = GroupBy.group(tuple, groupByFields);

			output.collect(tg, volume);
		}
	}

	/** Combiner class */
	public static class GroupByCombiner extends MapReduceBase implements
		Reducer<TextGroup, FloatWritable, TextGroup, FloatWritable> {

		public void reduce(TextGroup key, Iterator<FloatWritable> values,
				OutputCollector<TextGroup, FloatWritable> output, Reporter reporter) throws IOException {

			// TODO: It is still hard code
			float revenue = 0;
			while (values.hasNext()) {
				revenue += values.next().get();
			}
			
			output.collect(key, new FloatWritable(revenue));
		}

	}
	
	/** Reducer class */
	public static class GroupByReducer extends MapReduceBase implements
		Reducer<TextGroup, FloatWritable, Text, FloatWritable> {

		public void reduce(TextGroup key, Iterator<FloatWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

			TPCH7OutputCollector tpch7Output = new TPCH7OutputCollector(output);
			
			// TODO: This is aggregation hard code
			float revenue = 0;
			while (values.hasNext()) {
				revenue += values.next().get();
			}

			tpch7Output.collect(key.get(0).toString(), key.get(1).toString(), 
					Integer.parseInt(key.get(2).toString()), revenue);
		}
	}

	/** Convenience method to made definitions shorter */
	public static JobConf createJob(Configuration conf_, Relation relation,
			List<SelectionEntry<String>> filters, List<String> projection,
			List<String> groupByFields, Relation outRelation) throws IOException {

		if (IS_LOCAL) {
			conf_.set("mapred.job.tracker", "local");
			conf_.set("fs.default.name", "file://///");
		}

		// Set parameters
		conf_ = SelectionFilter.addSelectionsToJob(conf_, PARAM_SELECTION, 
				filters, relation.schema);
		conf_ = ProjectionFilter.addProjectionsToJob(conf_,	PARAM_PROJECTION, 
				projection, relation.schema);
		
		
		conf_.setStrings(PARAM_ORDER_LIST, Arrays.asList(
				relation.schema.getColumnIndex("SHIPDATE")+"",
				relation.schema.getColumnIndex("EXTENDEDPRICE")+"",
				relation.schema.getColumnIndex("DISCOUNT")+"").toArray(new String[0]));
		
		List<String> columnIndexes = new ArrayList<String>();
		for (String entry : groupByFields) {
			columnIndexes.add(relation.schema.getColumnIndex(entry) + "");
		}
		conf_.setStrings(PARAM_GROUPBY, columnIndexes.toArray(new String[0]));
		System.out.println(columnIndexes.toString());

		JobConf conf = new JobConf(conf_);
		conf.setJobName("GroupBy");

		conf.setJarByClass(GroupBy.class);
		
		// Input & Output path
		FileInputFormat.setInputPaths(conf, new Path(relation.storageFileName));
		FileOutputFormat.setOutputPath(conf, new Path(outRelation.storageFileName));
		
		// Mapper classes
		conf.setMapperClass(GroupByMapper.class);

		// Mapper output class
		conf.setMapOutputKeyClass(TextGroup.class);
		conf.setMapOutputValueClass(FloatWritable.class);

		// Combiner class
		conf.setCombinerClass(GroupByCombiner.class);
		
		// Reducer
		conf.setReducerClass(GroupByReducer.class);
		
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);

		return conf;
	}
}

