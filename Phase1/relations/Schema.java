package relations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;

/**
 * Class for handling relation schemas.
 * 
 * Doesn't store the actual relation relations, but knows how to manipulate
 * rows.
 */
public class Schema {
	/** Schema hard code constants */
	public static List<String> NATIONS_FIELDS = Arrays.asList(
			"NATIONKEY", "NAME", "REGIONKEY", "COMMENT" );
	public static List<String> SUPPLIER_FIELDS = Arrays.asList(
			"SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE",
			"ACCTBAL", "COMMENT" );
	public static List<String> CUSTOMER_FIELDS = Arrays.asList(
			"CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE",
			"ACCTBAL", "MKTSEGMENT", "COMMENT" );
	public static List<String> LINEITEM_FIELDS = Arrays.asList(
			"ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER",
			"QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG",
			"LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE",
			"SHIPINSTRUCT", "SHIPMODE", "COMMENT");
	public static List<String> ORDERS_FIELDS = Arrays.asList( "ORDERKEY",
			"CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE", "ORDERPRIORITY",
			"CLERK", "SHIPPRIORITY", "COMMENT");
	public static List<String> N1_SUPPLIER_FIELDS = Arrays.asList("NAME", 
			"SUPPKEY");
	public static List<String> N1_SUPPLIER_LINEITEM_FIELDS = Arrays.asList(
			"NAME", "ORDERKEY", "EXTENDEDPRICE", "DISCOUNT", "SHIPDATE");

	/** Constants */
	public static final String COLUMN_SEPARATOR_RE = "\\|";
	public static final String COLUMN_SEPARATOR = "|";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	
	/** Data types */
	public static final int STRING = 0;
	public static final int INT = 1;
	public static final int DOUBLE = 2;
	public static final int DATE = 3;	
	
	/** schema fields */
	private List<String> _schema;
	
	/** schema fields datatype*/
	protected HashMap<String, Integer> _fieldsDataType;
	
	/**
	 * Create a schema from a list of column names.
	 * by default everything is STRING
	 */
	public Schema(List<String> schema) {
		_schema = schema;
		
		_fieldsDataType = new HashMap<String, Integer>();
		for (String field : _schema) {
			_fieldsDataType.put(field, STRING);
		}
	}
	
	/** Create a schema from a list of column names and types */
	public Schema(List<String> schema, HashMap<String, Integer> fieldsDataType) {
		_schema = schema;
		_fieldsDataType = fieldsDataType;
	}
	
	public int setDataType(String field, int dataType) {
		return _fieldsDataType.put(field, dataType);
	}
	
	/** @return data type of field */
	public int getDataType(int fieldIndex) {
		return _fieldsDataType.get(_schema.get(fieldIndex));
	}
	
	/** @return data type of field */
	public int getDataType(String field) {
		return _fieldsDataType.get(field);
	}
	
	/** @return schema fields */
	public List<String> getFields() {
		return _schema;
	}

	/** Create a new schema as a projection of the current schema. */
	public Schema projection(List<String> projection) {
		// Get alias
		String alias = projection.get(projection.size()-1);
		
		HashMap<String, Integer> fieldsDataType = new HashMap<String, Integer>();
		ArrayList<String> proj = new ArrayList<String>();
		for (int i = 0; i < projection.size()-1; i++) {
			String n_field = alias + projection.get(i);
			proj.add(n_field); 
			fieldsDataType.put(n_field, _fieldsDataType.get(projection.get(i)));
		}
		return new Schema(proj, fieldsDataType);
	}

	/** Create a new schema as a join of two existing schemas */
	public Schema join(Schema rhs) {
		ArrayList<String> joinFields = new ArrayList<String>(_schema.size() + rhs._schema.size());
		joinFields.addAll(_schema);
		joinFields.addAll(rhs._schema);
		
		HashMap<String, Integer> fieldsDataType = new HashMap<String, Integer>();
		fieldsDataType.putAll(_fieldsDataType);
		fieldsDataType.putAll(rhs._fieldsDataType);
		
		return new Schema(joinFields, fieldsDataType);
	}

	/** Get the index of a column by column name. */
	public int getColumnIndex(String col) {
		return _schema.indexOf(col);
	}
	
	//==============================================
	// THESE BELOW METHODS CURRENTLY ARE NOT USED
	//==============================================
	
	/** Convenience function: get a sorted list of column indices. */
	public List<Integer> getColumnIndices(List<String> colNames) {
		ArrayList<Integer> result = new ArrayList<Integer>(colNames.size());
		for (String col : colNames)
			result.add(getColumnIndex(col));
		Collections.sort(result);
		return result;
	}

	/** Extract a value from column col (index starting at 0) of a row. */
	public Text getValue(Text row, int index) {
		int startIndex = 0, endIndex = -1;
		for (int count = index; count >= 0; --count) {
			startIndex = endIndex + 1;

			// find the next | character
			endIndex = row.find("|", startIndex);
			if (endIndex < 0) {
				if (count == 0)
					// OK, we return the last item
					endIndex = row.getLength();
				else
					// error: our tuple doesn't have enough columns
					// TODO: how, if at all, should we report?
					return new Text();
			}
		}

		Text result = new Text();
		result.set(row.getBytes(), startIndex, endIndex - startIndex);
		return result;
	}

	/**
	 * Projection: return a Text item of the specified set of columns.
	 * 
	 * Indices in columns must be sorted.
	 */
	public Text rowProjection(Text row, List<Integer> columns) {
		Text result = new Text();

		boolean first = true;
		int lastCol = 0;
		int endIndex = -1;
		for (Integer column : columns) {
			int startIndex = 0;
			for (; lastCol <= column; ++lastCol) {
				startIndex = endIndex + 1;

				// find the next | character
				endIndex = row.find("|", startIndex);
				if (endIndex < 0) {
					if (lastCol == column)
						// OK, we return the last item
						endIndex = row.getLength();
					else {
						// error: our tuple doesn't have enough columns
						// TODO: how, if at all, should we report?
						endIndex = startIndex;
						break;
					}
				}
			}

			if (!first) {
				// some other entry came before, which means both that we want
				// to add a divider and that our input starts with a divider
				startIndex -= 1;
				if (startIndex < 0)
					// with the exception of some very weird bugs
					startIndex = 0;
			}
			result.append(row.getBytes(), startIndex, endIndex - startIndex);
			first = false;
		}

		return result;
	}

	/**
	 * Return the join on a row. Simple concatenation with separator; it is
	 * assumed that columns to be joined have been removed from at least one
	 * tuple.
	 */
	public Text rowJoin(Text row1, Text row2) {
		Text result = new Text(row1);
		byte[] sep = {'|'};
		result.append(sep, 0, 1);
		result.append(row2.getBytes(), 0, row2.getLength());
		return result;
	}

	public String toString() {
		return "Schema: " + _schema.toString();
	}

	// unit test
	public static void main(String[] args) throws Exception {
		List<String> names123 = Arrays.asList( "col1", "col2", "col3" );
		List<String> names13 = Arrays.asList( "col1", "col3" );

		Schema schema = new Schema(names123);
		Schema schema13 = new Schema(names13);
	
		List<String> proj1 = Arrays.asList( "col1", "col2", "1_" );
		List<String> proj2 = Arrays.asList( "col1", "" );
		List<String> joinList = Arrays.asList( "1_col1", "1_col2", "col1" );
		
		Schema join = schema.projection(proj1).join(schema13.projection(proj2));
		
		for (int i = 0; i < joinList.size(); i++) {
			System.out.println(join.getFields().get(i));
			assert(join.getFields().get(i).equals(joinList.get(i)));
		}
		
		System.out.println("All Tests OK");
	}
}
