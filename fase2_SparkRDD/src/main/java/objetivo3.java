import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

public class objetivo3 {

    public static Dataset<Row> base(SparkSession spark, Dataset<Row> title_basics, Dataset<Row> title_ratings, Dataset<Row> title_principal, Dataset<Row> name_basics){
        //Base:Nome, idade, número de títulos em que participou, intervalo de anos de atividade eclassificação média dos títulos em que participa.

        //agrupar o title_basics ao title_principal atraves do id do filme
        Dataset<Row> movies = title_basics.join(title_principal, title_basics.col("tconst2").equalTo(title_principal.col("tconst")), "inner");
        movies=movies.drop("tconst2");

        //agrupar os resultados da query anterior com os respetivos ratings de cada titulo
        movies = movies.join(title_ratings, movies.col("tconst").equalTo(title_ratings.col("tconst2")), "inner");
        movies=movies.drop("tconst2");

        //calcular o ano do primeiro e ultimo titulo feitos por cada ator
        //calcular a média da avaliação de todos os filmes de cada ator
        movies.createOrReplaceTempView("movies");
        movies = spark.sql("select nconst, min(startYear) as first, max(startYear) as last, avg(averageRating) as averageRating," +
                " count(*) as n_titles  from movies group by nconst");

        //agrupar os resultados da query anterior com o name_basics
        movies = movies.join(name_basics, movies.col("nconst").equalTo(name_basics.col("nconst2")),"inner");
        movies = movies.drop("nconst2");

        movies.createOrReplaceTempView("movies");

        //calcular a idade de cada actor
        movies= spark.sql("select nconst, primaryName, n_titles, first, last, averageRating, SUBSTRING(birthYear, 0, CHAR_LENGTH(birthYear)-1) as decade, " +
                "case when deathYear == '\\\\N' then  YEAR(current_date()) - birthYear " +
                "else deathYear-birthYear " +
                "end as age " +
                "from movies order by n_titles desc");

        return movies;
    }

    public static  Dataset<Row> hits(SparkSession spark, Dataset<Row> title_ratings,Dataset<Row> title_principal){
        //Hits:Top 10 dos títulos mais bem classificados em que participo.

        //agrupar o title_principal ao title_ratings de forma a obter a avaliação por titulo
        Dataset<Row> movies = title_principal.join(title_ratings, title_principal.col("tconst").equalTo(title_ratings.col("tconst2")), "inner");
        movies=movies.drop("tconst2");

        movies.createOrReplaceTempView("movies");

        //calcular os 10 titulos mais bem classificados em que cada actor participou
        movies = spark.sql("select * from ( select nconst, tconst, averageRating, " +
                " row_number() over (partition by nconst order by averageRating desc) as title_actor_rank " +
                " from movies) ranks " +
                " where title_actor_rank <= 10");

        movies=movies.groupBy("nconst").agg(collect_list(col("tconst")).as("hits"));
        return movies;
    }

    public static  Dataset<Row> generation(SparkSession spark,Dataset<Row> title_ratings,Dataset<Row> title_principal,Dataset<Row> name_basics){
        //Generation:Nomes do top 10 de atores da mesma geração (i.e., que nasceram na mesma década).

        Dataset<Row> movies = title_ratings.join(title_principal, title_ratings.col("tconst2").equalTo(title_principal.col("tconst")), "inner");
        movies=movies.drop("tconst2");

        movies.createOrReplaceTempView("movies");

        movies = spark.sql("select nconst, avg(averageRating) as averageRating from movies group by nconst");

        movies = movies.join(name_basics, movies.col("nconst").equalTo(name_basics.col("nconst2")),"inner");
        movies=movies.drop("nconst2");
        movies=movies.drop("deathYear");

        movies.createOrReplaceTempView("movies");

        movies = spark.sql("select * from ( select SUBSTRING(birthYear, 0, CHAR_LENGTH(birthYear)-1) as decade, nconst, " +
                " row_number() over (partition by SUBSTRING(birthYear, 0, CHAR_LENGTH(birthYear)-1) order by averageRating desc) as actor_rank " +
                " from movies) ranks " +
                " where actor_rank <= 10 order by decade desc, actor_rank desc");

        movies.drop("actor_rank");

        movies=movies.groupBy("decade").agg(collect_list(col("nconst")).as("generation"));

        return movies;
    }

    public static  Dataset<Row> friends(SparkSession spark, Dataset<Row> title_principal){
        //Friends:Conjunto de colaboradores de cada ator (i.e., outros atores que participaram nosmesmos títulos).

        //calcular os actores com quem cada actor colaborou
        title_principal.createOrReplaceTempView("title_principal");
        Dataset<Row> friends = spark.sql("SELECT y.*, x.nconst as friend FROM title_principal x" +
                "  JOIN title_principal y ON y.tconst = x.tconst" +
                "   AND y.nconst <> x.nconst" +
                " order by x.tconst");

        friends=friends.drop("tconst");
        friends=friends.drop("ordering");
        friends=friends.drop("category");
        friends=friends.drop("job");
        friends=friends.drop("characters");

        //agrupar os resultados por actor criando uma lista com os atores com quem colaborou
        friends=friends.groupBy("nconst").agg(collect_list(col("friend")).as("friends"));

        return friends;
    }

    public static void main(String[] args) throws Exception {

        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.OFF);
        }

        SparkSession spark = SparkSession.builder()
                .appName("example")
                .master("local")
                .getOrCreate();

        //Leitura dos dados armazenamos na alinea 1 em .csv
        Dataset<Row> title_basics = spark.read().format("csv").option("header", "true").option("delimiter", "\t").load("title.basics");

        Dataset<Row> title_ratings = spark.read().format("csv").option("header", "true").option("delimiter", "\t").load("title.ratings");

        Dataset<Row> title_principal = spark.read().format("csv").option("header", "true").option("delimiter", "\t").load("title.principals");

        Dataset<Row> name_basics = spark.read().format("csv").option("header", "true").option("delimiter", "\t").load("name.basics");

        System.out.println("--------------------------------Schema do title_basics--------------------------------");
        title_basics.printSchema();
        System.out.println("--------------------------------Schema do title_ratings--------------------------------");
        title_ratings.printSchema();
        System.out.println("--------------------------------Schema do title_principal--------------------------------");
        title_principal.printSchema();
        System.out.println("--------------------------------Schema do name_basics--------------------------------");
        name_basics.printSchema();

        //selecionar de cada dataframe os atributos que interessam usar
        title_basics = title_basics.select(title_basics.col("tconst").alias("tconst2"),
                title_basics.col("startYear"));

        title_ratings= title_ratings.select(title_ratings.col("tconst").alias("tconst2"),
                title_ratings.col("averageRating"));

        name_basics = name_basics.select(name_basics.col("nconst").alias("nconst2"),
                name_basics.col("primaryName"),
                name_basics.col("birthYear"),
                name_basics.col("deathYear"),
                name_basics.col("primaryProfession"));

        //guardar os dados de modo a que possamos aceder atraves das queries sql
        title_basics.createOrReplaceTempView("title_basics");
        title_ratings.createOrReplaceTempView("title_ratings");
        title_principal.createOrReplaceTempView("title_principal");
        name_basics.createOrReplaceTempView("name_basics");
        name_basics = spark.sql("select * from name_basics where primaryProfession='actress' or  primaryProfession='actor' ");

        //obter Nome, idade, número de títulos em que participou, intervalo de anos de atividade eclassificação média dos títulos em que participa.
        Dataset<Row> base = base(spark, title_basics, title_ratings, title_principal, name_basics);
        //obter Hits:Top 10 dos títulos mais bem classificados em que participou.
        Dataset<Row> hits = hits(spark, title_ratings, title_principal);
        //obter Generation:Nomes do top 10 de atores da mesma geração (i.e., que nasceram na mesmadécada).
        Dataset<Row> friends = friends(spark, title_principal);
        //obter Friends:Conjunto de colaboradores de cada ator (i.e., outros atores que participaram nosmesmos títulos).
        Dataset<Row> generation= generation(spark, title_ratings,title_principal, name_basics);

        hits=hits.withColumnRenamed("nconst", "nconst2");
        friends=friends.withColumnRenamed("nconst", "nconst2");

        //agrupar os resultados dos base com o generation
        Dataset<Row> data = base.join(generation, base.col("decade").equalTo(generation.col("decade")), "inner");

        data=data.drop("decade");
        //agrupar os resultados da query anterior com os hits
        data = data.join(hits, data.col("nconst").equalTo(hits.col("nconst2")), "inner");

        //agrupar os resultados da query anterior com os resultados dos friends
        data = data.join(friends, data.col("nconst").equalTo(friends.col("nconst2")), "inner");
        data=data.drop("nconst2");

        //guardar os dados no formato parquet
        data.write().parquet("result.parquet");

        data.printSchema();

        data.show();

     }
}
