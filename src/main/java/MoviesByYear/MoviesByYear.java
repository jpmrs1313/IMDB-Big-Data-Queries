package MoviesByYear;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;


public class MoviesByYear {

    //Função que vai ser usada mais para a frente para order uma estrutura Map
    private static Map<Integer, Float> sortByValue(Map<Integer, Float> unsortMap) {

        // 1. Convert Map to List of Map
        List<Map.Entry<Integer, Float>> list =
                new LinkedList<Map.Entry<Integer, Float>>(unsortMap.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>() {
            public int compare(Map.Entry<Integer, Float> o1,
                               Map.Entry<Integer, Float> o2) {
                //return (o1.getValue()).compareTo(o2.getValue()); change order to sort ascending
                return (o2.getValue()).compareTo(o1.getValue()); // sort descending
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<Integer, Float> sortedMap = new LinkedHashMap<Integer, Float>();
        for (Map.Entry<Integer, Float> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    // Config parquet file projection (projetar só as colunas que queremos)
    private static Configuration conf;
    public static  Schema getSchemapro() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path schema = new Path("hdfs:///schema_projection.parquet");
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
    // Config parquet file output
    public static  Schema getSchema() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path schema = new Path("hdfs:///schema_output.parquet");
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

    public static class FromParquetMapper extends Mapper<Void, GenericRecord, Text, Text>{

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            //Criar a sub schema do schema principal
            List<GenericRecord> title = (List<GenericRecord>) value.get("titles");
            //Fazer a consulta ao sub schema e guardar na tipologia
            String tipologia =title.get(0).get(0).toString();

            //Filtrar só pelos filmes sem o ano a nulo
            if(value.get(3)!=null && tipologia.equals("movie"))  {
                //Passar o Ano para Key e tudo o resto(necessáro) como value do map
                String s = (String) value.get(0);
                s= s+"\t"+title.get(0).get(0).toString();
                s= s+"\t"+title.get(0).get(1).toString();
                s= s+"\t"+title.get(0).get(2).toString();
                s = s+"\t"+ value.get(1);
                s = s+"\t"+ value.get(7);
                s = s+"\t"+ value.get(8);

                String ano =value.get(3).toString();

                context.write(new Text(ano), new Text(s));
            }
        }
    }

    public static class FromParquetReducer extends Reducer<Text,Text,Void, GenericRecord> {
        //Declare Variables to Schema output
        private Schema schema;
        private Schema tschema;
        private Schema mschema;

        //Executar a configuração do schema output

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //Principal Schema
            schema = getSchema();
            tschema = schema.getField("filmRank").schema().getElementType();
            mschema = schema.getField("bestMovies").schema().getElementType();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //variavel para guardar o numero total de filmes
            int total=0;
            //guardar os votos do ultimo filme com mais votos
            int max_votos=0;
            // variavel para guardar o filme com mais votos
            String movie_max_votos = null;
            //Array List para guardar o nome dos filmes
            ArrayList <String> sub_rank = new ArrayList<>();
            //Array list para guardar os top 10 filmes
            ArrayList <String> rank = new ArrayList<>();
            //Array List para guardar os valores do rank
            ArrayList <Float> rv = new ArrayList<>();
            //Array List para guardar os valores dos top 10 rankings
            ArrayList <Float> rankvalues = new ArrayList<>();
            //Map que vai ter os dados do array list ordenados
            Map<Integer, Float> RatingsMap = new HashMap<>();


            for(Text value: values) {
                //total de filmes nesse ano
                total += 1;
                //split na linha
                String[] movie = value.toString().split("\t");
                //numero de votos
                int votes=Integer.parseInt(movie[6]);
                //rating
                float rating=Float.parseFloat(movie[5]);
                //nome do filem
                String movie_name = movie[3];
                // se o maximo de votos for superado muda o pódio
                if(votes>=max_votos)
                {
                    movie_max_votos=movie_name;
                    max_votos = votes;
                }
                //inserir os valores no map
                RatingsMap.put(total,rating);
                //inserir os filmes
                sub_rank.add(movie_name);
                // inserir os ratings
                rv.add(rating);
            }
            //funcao que ordena os rankings
            RatingsMap=sortByValue(RatingsMap);
            //guarda o indice dos top 10 filmes
            List<Integer> keys = RatingsMap.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .limit(10)
                    .collect(Collectors.toList());

            //Adicionar ao Rank os principais filmes
            for (int indice:keys){
                rank.add(sub_rank.get(indice - 1));
                rankvalues.add(rv.get(indice -1));
            }


            //Insert output

            GenericRecord record = new GenericData.Record(schema);
            //insert year
            record.put("year", key.toString());
            //insert numFilms
            record.put("numFilms", total);
            //insert bestMovies
            List<GenericRecord> bestmovies = new ArrayList<>();
            GenericRecord mrecord = new GenericData.Record(mschema);
            mrecord.put("movie", movie_max_votos);
            mrecord.put("votes", max_votos);
            bestmovies.add(mrecord);

            record.put("bestMovies", bestmovies);

            //insert filmRank
            List<GenericRecord> filmranks = new ArrayList<>();
            //INserir os top 10 filmes do ano
            for (int i = 0; i < rank.size(); i++) {
                GenericRecord trecord = new GenericData.Record(tschema);
                int j = i+1;
                trecord.put("position", j);
                trecord.put("name", rank.get(i));
                trecord.put("rating", rankvalues.get(i));
                filmranks.add(trecord);

            }
            record.put("filmRank", filmranks);


            //output parquet
            context.write(null, record);
            //configurar o job e testar
        }
    }


    public static void moviesbyyear(String dat1) throws Exception{
        // Cria um novo Job
        conf = new Configuration();
        Job job = Job.getInstance(conf , "FromParquet");

        // Especificar vários parâmetros específicos do trabalho
        job.setJarByClass(MoviesByYear.class);
        job.setMapperClass(MoviesByYear.FromParquetMapper.class);
        job.setReducerClass(MoviesByYear.FromParquetReducer.class);

        //Configurar o Input
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(dat1));
        AvroParquetInputFormat.setRequestedProjection(job,getSchemapro());

        //Configurar o Output
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        //Configurar os tipos de dados que vão do mapper para o reduce
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Configurar a conversão dos dados para o ficheiro final
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job,getSchema());
        FileOutputFormat.setOutputPath(job, new Path("hdfs:///MoviesByYear"));

        // Configuração de execução
        job.waitForCompletion(true);
    }
}
