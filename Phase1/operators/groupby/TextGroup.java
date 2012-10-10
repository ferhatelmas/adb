package operators.groupby;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.*;

public class TextGroup implements WritableComparable<TextGroup> {
	/** group */
	private List<Text> group;

	public TextGroup() {
		group = new ArrayList<Text>();
	}

	public TextGroup(List<String> group) {
		this.group = new ArrayList<Text>();
		for (String text : group) {
			this.group.add(new Text(text));
		}
	}

	/** @return field at position i */
	public Text get(int i) {
		return group.get(i);	
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(group.size());
		for (Text text : group) {
			text.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
	    group = new ArrayList<Text>(size);
		for (int i = 0; i < size; i++) {
			Text text = new Text();
			text.readFields(in);
			group.add(text);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextGroup) {
			TextGroup tg = (TextGroup) o;
			
			if (tg.group.size() != group.size()) {
				return false;
			}

			for (int i = 0; i < tg.group.size(); i++) {
				if (!(tg.get(i)).equals(this.get(i))) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer();
		for (Text text : group) {
			s.append(text.toString() + "|");
		}
		
		if (s.length() > 0) {
			s.deleteCharAt(s.length()-1);
		}
		return s.toString();
	}

	@Override
	public int compareTo(TextGroup tg) {
		int cmp = 0;
		for (int i = 0; i < tg.group.size(); i++) {
			cmp = this.get(i).compareTo(tg.get(i));
			if (cmp != 0) {
				return cmp;
			}
		}
		return cmp;
	}
	
	//unit test
	public static void main(String[] args) {
		TextGroup tg1 = new TextGroup(Arrays.asList("1", "2", "3"));
		TextGroup tg2 = new TextGroup(Arrays.asList("1", "2", "3"));
		
		//System.out.println(tg1.compareTo(tg2));
		//System.out.println(tg2.hashCode());
		
		assert(tg1.equals(tg2));
		
		System.out.println("All tests are OK!");
	}
}
