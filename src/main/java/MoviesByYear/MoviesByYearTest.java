package MoviesByYear;

import ToMerge.ToMergeTest;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;

public class MoviesByYearTest {

    public static class FromParquetMapper extends Mapper<Void, GenericRecord, Void, Text>{

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            context.write(null,new Text(value.toString()));
        }
    }

    public static void moviesbyyeartest(String dat) throws Exception{
        // Cria um novo Job
        Job job = Job.getInstance(new Configuration(), "mbyt");

        // Especificar vários parâmetros específicos do trabalho
        job.setJarByClass(ToMergeTest.class);
        job.setMapperClass(ToMergeTest.FromParquetMapper.class);
        job.setNumReduceTasks(0);

        //Configurar o Input
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(dat));

        //Configurar o Output
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Text.class);

        //Configurar a conversão dos dados para o ficheiro final
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs:///MoviesByYearTest"));

        job.waitForCompletion(true);
    }
}
