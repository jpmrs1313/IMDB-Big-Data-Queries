package SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// classe que guarda que representa a chave dupla
public class CompositeKeyWritable implements Writable,
	WritableComparable<CompositeKeyWritable> {

	private String genre; // primeiro parametro da chave
	private Float rating; // segundo parametro da chave

	public CompositeKeyWritable() {
	}

	//instanciar uma chaves
	public CompositeKeyWritable(String genre, Float rating) {
		this.genre = genre;
		this.rating = rating;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(genre).append("\t")
				.append(rating.toString())).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		genre = WritableUtils.readString(dataInput);
		rating = (Float) dataInput.readFloat();
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, genre);
		dataOutput.writeFloat(rating);
	}

	//apenas é usado no GroupingComparator para comparar as chaves a enviar para cada tarefa de reduce
	public int compareTo(CompositeKeyWritable objKeyPair) {

		int result = genre.compareTo(objKeyPair.genre);
		if (0 == result) {
			result = rating.compareTo(objKeyPair.rating);
		}
		return 1*result; //ascending sort
		//return -1*result; //descending sort
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre= genre;
	}

	public Float getRating() {
		return rating;
	}

	public void setRating(Float rating) {
		this.rating = rating;
	}
}