package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortBasicCompKeySortComparator extends WritableComparator {

	protected SecondarySortBasicCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	//ordena através do segundo parametro das chaves, neste caso o rating
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getGenre().compareTo(key2.getGenre());
		if (cmpResult == 0)// same genre
		{
			//return key1.getRating().compareTo(key2.getRating()); //ascendente
			return -key1.getRating().compareTo(key2.getRating()); //descendente

		}
		return cmpResult;
	}
}