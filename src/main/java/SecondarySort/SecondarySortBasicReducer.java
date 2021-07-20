package SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortBasicReducer
  	extends
		Reducer<CompositeKeyWritable, Text, Text, Text> {
	
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String best_movie_by_gender  = values.iterator().next().toString();
		// para sugerir o segundo melhor filme ao melhor
		String II_best_movie_by_gender= values.iterator().next().toString();

		// guardar dados do primeiro e segundo filmes
		String dados_filme1 = key.getGenre() + ' ' + best_movie_by_gender;
		String dados_filme2 = key.getGenre() + ' ' + II_best_movie_by_gender;

		//enviar o primeiro e segundo melhor filme
		context.write(new Text(dados_filme1),new Text(II_best_movie_by_gender));
		context.write(new Text(dados_filme2),new Text(best_movie_by_gender));

		for (Text value : values) {

			String dados_filme = key.getGenre() + ' ' + value.toString() ;
			String filme_sugerido = best_movie_by_gender;

			context.write(new Text(dados_filme),new Text(filme_sugerido));
		}
	}
}