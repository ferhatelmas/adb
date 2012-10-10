package operators.group;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextTriple implements WritableComparable<TextTriple> {
	private Text first;
	private Text second;
	private Text third;

	public TextTriple() {
		set(new Text(), new Text(), new Text());
	}

	public TextTriple(String first, String second, String third) {
		set(new Text(first), new Text(second), new Text(third));
	}

	public TextTriple(Text first, Text second, Text third) {
		set(first, second, third);
	}

	public TextTriple(TextTriple tt) {
		set(new Text(tt.first), new Text(tt.second), new Text(tt.third));
	}

	public void set(Text first, Text second, Text third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public Text getThird() {
		return third;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode()*63 + third.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextTriple) {
			TextTriple tt = (TextTriple) o;
			return first.equals(tt.first) && second.equals(tt.second) && third.equals(tt.third);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second + "\t" + third;
	}

	@Override
	public int compareTo(TextTriple tt) {
		int cmp = first.compareTo(tt.first);
		if (cmp != 0) {
			return cmp;
		}
		cmp = second.compareTo(tt.second);
		if (cmp != 0) {
			return cmp;
		}
		return third.compareTo(tt.third);
	}

}
