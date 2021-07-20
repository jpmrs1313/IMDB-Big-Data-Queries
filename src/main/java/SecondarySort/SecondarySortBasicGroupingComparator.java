package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// controls which keys are grouped together for a single call to the Reducer.reduce() function.
public class SecondarySortBasicGroupingComparator extends WritableComparator {
  protected SecondarySortBasicGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getGenre().compareTo(key2.getGenre());
	}
}