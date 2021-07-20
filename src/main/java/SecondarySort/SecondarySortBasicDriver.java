package SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;

public class SecondarySortBasicDriver {

	public static void secondarysort(String input_path) throws Exception {
		// Cria um novo Job
		Job job = Job.getInstance(new Configuration());
		job.setJobName("Secondary sort");

		// Especificar vários parâmetros específicos do trabalho
		job.setJarByClass(SecondarySortBasicDriver.class);
		job.setPartitionerClass(SecondarySortBasicPartitioner.class);
		job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
		job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
		job.setMapperClass(SecondarySortBasicMapper.class);
		job.setReducerClass(SecondarySortBasicReducer.class);

		//Define o número de tarefas de Reduce a ocorrer, por omissão é 1
		//29 porque é igual ao numero total de generos
		job.setNumReduceTasks(29);

		//Configurar o Input
		job.setInputFormatClass(AvroParquetInputFormat.class);
		AvroParquetInputFormat.addInputPath(job, new Path("hdfs:///Output"));
		AvroParquetInputFormat.setRequestedProjection(job,SecondarySortBasicMapper.getSchema());

		//Configurar o output do Map
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(Text.class);

		//Configurar o Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Configurar a conversão dos dados para o ficheiro final
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("hdfs:///SecondarySort"));

		job.waitForCompletion(true);
	}
}