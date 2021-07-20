package ToMerge;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class ToMerge {

    // Config parquet file
        private static Configuration conf;
        public static  Schema getSchema() throws IOException {
            FileSystem fs = FileSystem.get(new Configuration());
            Path schema = new Path("hdfs:///schema.parquet");
            FSDataInputStream in = fs.open(schema);

            StringBuilder strout =new StringBuilder();
            byte[] buffer=new byte[4096];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) > 0)
                strout.append(new String(buffer, 0, bytesRead));
            in.close();

            MessageType mt = MessageTypeParser.parseMessageType(strout.toString());

            return new AvroSchemaConverter().convert(mt);
        }


    public static class MapperSideJoin extends Mapper<LongWritable, Text, Void, GenericRecord> {
        //Declare Schema and tschema
        private Schema schema;
        private Schema tschema;
        private static Map<String, List<String>> filmsMap = new HashMap<>();


        @Override
        protected void setup(Context context) throws IOException {
            //Declare Schema and tschema
            schema = getSchema();
            tschema = schema.getField("titles").schema().getElementType();

            //Percorrer ficheiros em Cache
            URI[] mapsideFiles = context.getCacheFiles();
            for (URI u : mapsideFiles) {
                try {
                    loadRatingsMap(u);
                } catch (CompressorException e) {
                    e.printStackTrace();
                }
            }
        }
        private void loadRatingsMap(URI u) throws CompressorException, IOException {

            //Faz o decompress inserindo no BR e vai linha a linha colocar os dados no MAP;
            //FICHEIRO EM MEMORIA
            Path pt = new Path(u.getPath());
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            br.readLine();
            String s;
            String[] token;

            //Guardar na primeira posição do filmsmap o equivalente a key do ficheiro principal
            while ((s = br.readLine()) != null){

                token = s.split("\t");
                List<String> l = new ArrayList<>();
                l.add(token[1]);
                l.add(token[2]);
                filmsMap.put(token[0], l);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //Retirar o cabeçalho
            if (key.get() != 0) {

                //Guardar o schema no record
                GenericRecord record = new GenericData.Record(schema);

                String[] s = value.toString().split("\t");

                //Armazenar no campo tconst do registo
                record.put("tconst", s[0]);

                //Armazenar o group titles
                List<GenericRecord> titles = new ArrayList<>();
                GenericRecord trecord = new GenericData.Record(tschema);
                trecord.put("type", s[1]);
                trecord.put("primary", s[2]);
                trecord.put("original", s[3]);
                titles.add(trecord);

                record.put("titles", titles);

                //Armazenar no campo isAdult
                record.put("isAdult", Integer.parseInt(s[4]));

                //Armazenar no campo startYear
                if (!s[5].equals("\\N"))
                    record.put("startYear", Integer.parseInt(s[5]));

                //Armazenar no campo endYear
                if (!s[6].equals("\\N"))
                    record.put("endYear", Integer.parseInt(s[6]));

                //Armazenar no campo runtimeMinutes
                if (!s[7].equals("\\N"))
                    record.put("runtimeMinutes", Integer.parseInt(s[7]));

                //Armazenar no campo List genres
                List<String> genres = new ArrayList<>();
                Collections.addAll(genres, s[8].split(","));
                record.put("genres", genres);

                //PROCURA O FILME NOS DADOS EM MEMORIA, SE NÃO ENCONTRAR RETORNA ZEROS
                //inserir rating e votos
                if(filmsMap.get(s[0]) != null) {
                    record.put("averageRating", Float.parseFloat(filmsMap.get(s[0]).get(0)));
                    record.put("numVotes", Integer.parseInt(filmsMap.get(s[0]).get(1)));
                }
                else{
                    record.put("averageRating", 0);
                    record.put("numVotes", 0);
                }
                context.write(null, record);

            }
        }
    }

    public static void tomerge() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // Cria um novo Job

        Job job = Job.getInstance(new Configuration(), "toMerge");

        // Especificar vários parâmetros específicos do trabalho
        job.setJarByClass(ToMerge.class);
        job.setMapperClass(MapperSideJoin.class);
        job.setNumReduceTasks(0);



        //Configurar o Input
        job.addCacheFile( new URI("hdfs:///title.ratings.tsv"));
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs:///title.basics.tsv"));

        //Configurar o Output
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        //Configurar a conversão dos dados para o ficheiro final
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job,getSchema());
        FileOutputFormat.setOutputPath(job, new Path("hdfs:///Output"));

        // Configuração de execução
        job.waitForCompletion(true);

    }
}
