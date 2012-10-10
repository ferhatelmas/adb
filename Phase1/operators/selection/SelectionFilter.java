package operators.selection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import relations.Schema;
import org.apache.hadoop.conf.Configuration;

public class SelectionFilter {

	/** logic operators */
	public static final int AND = 1;
	public static final int OR = 0;

	/** parameters passed to Mapred job */
	private static final String CONF_KEY_VALUES = "selection_values";
	private static final String CONF_KEY_COLUMNS = "selection_cols";
	private static final String CONF_KEY_CTRLS = "selection_ctrls";

	/** selection list*/
	ArrayList<SelectionEntry<Integer>> selectionArguments;	

	/**
	 * Selection implementation that checks for equality.
	 * logical expression computation is applying 
	 * 
	 * @param tuple
	 * @return tuple is selected or not
	 */

	public boolean checkSelection(String[] tuple) {	
		boolean isSelected = true;
		
		Stack<Boolean> operand = new Stack<Boolean>();
		
		operand.push(isSelected);

		for (SelectionEntry<Integer> entry : selectionArguments) {
			if (entry.getLogicType() == AND) {
				boolean topOperand = operand.pop();
				operand.push(topOperand && entry.isSelected(tuple[entry.getKey()]));
			} else {
				operand.push(entry.isSelected(tuple[entry.getKey()]));
			}
		}
		
		isSelected = operand.pop();
		while (!operand.isEmpty()) {
			isSelected = operand.pop() || isSelected;
		}

		return isSelected;
	}

	/**
	 * Constructor given Configuration. as join have multiple relations we add
	 * relationPrefix field
	 */
	public SelectionFilter(Configuration conf, String relationPrefix) { 
		String[] selectionValues = conf.getStrings(relationPrefix + CONF_KEY_VALUES, new String[] {});
		String[] sSelectionCols = conf.getStrings(relationPrefix + CONF_KEY_COLUMNS, new String[] {});
		String[] selectionCtrls = conf.getStrings(relationPrefix + CONF_KEY_CTRLS, new String[] {});

		selectionArguments = new ArrayList<SelectionEntry<Integer>>();

		for (int i = 0; i < selectionValues.length; i++) {
			selectionArguments.add(new SelectionEntry<Integer>(Integer.parseInt(sSelectionCols[i]),
					selectionValues[i], selectionCtrls[i]));
		}
	}

	public static Configuration addSelectionsToJob(Configuration conf, String relationPrefix,
			List<SelectionEntry<String>> selections, Schema schema) {
		if (selections != null) {
			System.out.println("SelectionFilterCreate n = " + selections.size());

			// using schema convert column names into their indexes
			List<String> columnIndexes = new ArrayList<String>();
			List<String> values = new ArrayList<String>();
			List<String> ctrls = new ArrayList<String>();

			for (SelectionEntry<String> entry : selections) {
				columnIndexes.add(schema.getColumnIndex(entry.getKey()) + "");
				values.add(entry.getValue());
				ctrls.add(entry.getCtrl());
			}

			conf.setStrings(relationPrefix + CONF_KEY_COLUMNS, columnIndexes.toArray(new String[0]));
			conf.setStrings(relationPrefix + CONF_KEY_VALUES, values.toArray(new String[0]));
			conf.setStrings(relationPrefix + CONF_KEY_CTRLS, ctrls.toArray(new String[0]));
		}
		return conf;
	}
	
	// unit test
	public static void main(String[] args) {
		String relationPrefix = "prefix_";
		Schema lineItem = new Schema(Schema.LINEITEM_FIELDS);
		lineItem.setDataType("SHIPDATE", Schema.DATE);
		@SuppressWarnings("unchecked")
		List<SelectionEntry<String>> shipdateFilters = Arrays.asList(
				new SelectionEntry<String>("SHIPDATE", "1995-01-01", SelectionFilter.AND + 
						SelectionEntry.CTRL_DELIMITER + Schema.DATE +
						SelectionEntry.CTRL_DELIMITER + ">="), 
				new SelectionEntry<String>("SHIPDATE", "1996-12-31", SelectionFilter.AND +
						SelectionEntry.CTRL_DELIMITER + Schema.DATE +
						SelectionEntry.CTRL_DELIMITER + "<="),
				new SelectionEntry<String>("COMMENT", "comm1", SelectionFilter.OR +
						SelectionEntry.CTRL_DELIMITER + Schema.STRING +
						SelectionEntry.CTRL_DELIMITER + "="),
				new SelectionEntry<String>("QUANTITY", "1", SelectionFilter.AND +
						SelectionEntry.CTRL_DELIMITER + Schema.STRING +
						SelectionEntry.CTRL_DELIMITER + "="));
		
		Configuration conf = SelectionFilter.addSelectionsToJob(new Configuration(), relationPrefix, shipdateFilters, lineItem);
		
		SelectionFilter selection = new SelectionFilter(conf, relationPrefix);
		
		String[] tuple = ("O|P|S|L|Q|E|D|T|R|L|1995-01-01|C|R|S|S|comm2").split("\\|");
		assert(selection.checkSelection(tuple) == true);
		
		tuple = ("O|P|S|L|Q|E|D|T|R|L|1996-12-31|C|R|S|S|comm2").split("\\|");
		assert(selection.checkSelection(tuple) == true);
		
		tuple = ("O|P|S|L|Q|E|D|T|R|L|1995-11-11|C|R|S|S|comm2").split("\\|");
		assert(selection.checkSelection(tuple) == true);
		
		tuple = ("O|P|S|L|1|E|D|T|R|L|1997-01-01|C|R|S|S|comm1").split("\\|");
		assert(selection.checkSelection(tuple) == true);
		
		tuple = ("O|P|S|L|Q|E|D|T|R|L|1994-12-31|C|R|S|S|comm1").split("\\|");
		assert(selection.checkSelection(tuple) == false);
		
		System.out.println("All Tests OK");
	}
}
