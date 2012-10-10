package operators.projection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import relations.Schema;

public class ProjectionFilter {
	
	/** parameters passed to Mapred job */
	private static final String CONF_PROJ_FIELDS = "projection_fields";
	
	/** projection fields */
	protected List<Integer> projectionFields;
	
	/** constructor */
	public ProjectionFilter(Configuration conf, String relationPrefix) {
		projectionFields = new ArrayList<Integer>();
		String[] projFields = conf.getStrings(relationPrefix + CONF_PROJ_FIELDS, new String[] {});
		for (String field : projFields) {
			projectionFields.add(Integer.parseInt(field));
		}
	}
	
	/** projection */
	public String[] projection(String[] tuple) {
		List<String> projTuple = new ArrayList<String>();
		
		if (projectionFields.size() == tuple.length) {
			return tuple;
		}
		
		for (int field : projectionFields) {
			projTuple.add(tuple[field]); 
		}
		
		return projTuple.toArray(new String[0]);
	}
	
	/** projection considers joinCol */
	public String[] projection(String[] tuple, int joinCol) {
		List<String> projTuple = new ArrayList<String>();
		
		// Put join column at index 0
		projTuple.add(tuple[joinCol]);
		
		if (projectionFields.size() == tuple.length) {
			return tuple;
		}	
		for (int field : projectionFields) {
			projTuple.add(tuple[field]); 
		}
		return projTuple.toArray(new String[0]);
	}
	
	public static Configuration addProjectionsToJob(Configuration conf, String relationPrefix, 
			List<String> projections, Schema schema) {
		if (projections != null) {
			System.out.println("ProjectionFilterCreate n = " + projections.size());
	
			// using schema convert column names into their indexes
			List<String> columnIndexes = new ArrayList<String>();
	
			for (String entry : projections) {
				int colIndex = schema.getColumnIndex(entry);
				if (colIndex != -1) {
					columnIndexes.add(colIndex + "");
				}
			}
			conf.setStrings(relationPrefix + CONF_PROJ_FIELDS, columnIndexes.toArray(new String[0]));
		}
		
		return conf;
	}
	
	// unit test
	public static void main(String[] args) throws Exception {
		String relationPrefix = "prefix_";
		Schema supplier = new Schema(Schema.SUPPLIER_FIELDS);
		List<String> projection = Arrays.asList(
				"SUPPKEY", "NATIONKEY", "ACCTBAL", "COMMENT", "l_" );
		
		Configuration conf = ProjectionFilter.addProjectionsToJob(new Configuration(), relationPrefix, projection, supplier);
		ProjectionFilter proj = new ProjectionFilter(conf, relationPrefix);
		
		String[] tuple = "SUPPKEY|NAME|ADDRESS|NATIONKEY|PHONE|ACCTBAL|COMMENT".split("\\|");
		String[] projTuple = proj.projection(tuple, 0);
		for (int i = 1; i < projTuple.length; i++) {
			//System.out.println(projTuple[i]);
			assert(projection.get(i-1).equals(projTuple[i]));
		}
		
		System.out.println("All Tests OK");
	}
}
