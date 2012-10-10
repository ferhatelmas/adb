package operators.selection;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import relations.Schema;

public class SelectionEntry<K> {
	/** delimiter constants*/
	public static final String CTRL_DELIMITER_RE = "\\|";
	public static final String CTRL_DELIMITER = "|";
	
	/** selection value */
	String value;
	
	/** selection field*/
	K key;
	
	/**	AND or OR operators */
	int logicType;	
	
	/** datatype of the field*/
	int dataType;
	
	/** comparison operator */
	boolean isEqual, isGreater, isLess;

	/** 
	 * constructor 
	 * ctrl (control code) usage: <logicType>|<dataType>|<comparison>
	 */
	public SelectionEntry(K key, String value, String ctrl) {
		this.key = key;
		this.value = value;
		
		String[] ctrls = ctrl.split(CTRL_DELIMITER_RE);
		logicType = Integer.parseInt(ctrls[0]);
		dataType = Integer.parseInt(ctrls[1]);
		
		isEqual = (ctrls[2].contains("="))? true : false;
		isGreater = (ctrls[2].contains(">"))? true : false;
		isLess = (ctrls[2].contains("<"))? true : false;
	}
	
	/** selection check */
	public boolean isSelected(String value) {
		switch (dataType) {
		case Schema.STRING:
			return (isEqual && value.compareTo(this.value) == 0)
				|| (isGreater && value.compareTo(this.value) > 0)
				|| (isLess && value.compareTo(this.value) < 0);
			
		case Schema.INT: case Schema.DOUBLE:
			return (isEqual && Double.parseDouble(value) == Double.parseDouble(this.value)) 
				|| (isGreater && Double.parseDouble(value) > Double.parseDouble(this.value))
				|| (isLess && Double.parseDouble(value) < Double.parseDouble(this.value));
			
		case Schema.DATE:
			DateFormat df = new SimpleDateFormat(Schema.DATE_FORMAT);
			try {
				return (isEqual && df.parse(value).equals(df.parse(this.value)))
					|| (isGreater && df.parse(value).after(df.parse(this.value)))
					|| (isLess && df.parse(value).before(df.parse(this.value)));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	/** @return control code */
	public String getCtrl() {
		String equation = "";
		if (isGreater) {
			equation = ">";
		}
		if (isLess) {
			equation = "<";
		}
		if (isEqual) {
			equation += "=";
		}
		
		return logicType + CTRL_DELIMITER + dataType + CTRL_DELIMITER +	equation;
	}

	public String setValue(String value) {
		return value;
	}

	public String getValue() {
		return value;
	}

	public K getKey() {
		return key;
	}

	public int getLogicType() {
		return logicType;
	}
	
	public int getDataType() {
		return dataType;
	}
	
	public boolean isEqual() {
		return isEqual;
	}
	
	public boolean isGreater() {
		return isGreater;
	}
	
	public boolean isLess() {
		return isLess;
	}
}