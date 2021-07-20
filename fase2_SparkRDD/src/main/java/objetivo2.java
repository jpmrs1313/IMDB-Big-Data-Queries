import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class objetivo2 {

    //itera pela lista de genres duma decada, agrupa por generos e conta-os
    private static Iterable<Tuple2<String, Integer>> countGenrePerDecade(Iterable<String> p) {

        Map<String,Integer> map = new HashMap<>();

        for (String s : p) {
            if (map.containsKey(s))
                map.put(s, map.get(s) + 1);
            else
                map.put(s, 1);
        }
        List<Tuple2<String,Integer>> ret = new ArrayList<>();
        map.forEach((k, v) -> ret.add(new Tuple2<>(k,v))); //converte de Map para List de Tuple2
        return ret;
    }

    //itera pela lista, apanha o genero mais comum e retorna apenas esse Tuple2
    private static Tuple2<String, Integer> getBestGenreDecade(Iterable<Tuple2<String, Integer>> p) {

        String genre = " ";
        Integer count = 0;

        for (Tuple2<String,Integer> t : p)
            if(t._2 > count) {
                genre = t._1;
                count = t._2;
            }

        return new Tuple2<>(genre,count);
    }

    //todos os anos entre xxx0 e xxx9 passam todos para xxx0 para indicar a decada
    private static Integer round(int original) {
        return original - (original % 10); // 2019 % 10 = 9 -> 2019 - 9 = 2010 (decada de 10)
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("g4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> actorsNames = sc.textFile("name.basics/part-00000")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("nconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1])) //identificador do ator, nome do ator
                .cache()
                ;

        JavaPairRDD<String, String> filmRatings = sc.textFile("title.ratings/part-00000")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]))//identificador do filme, rating do filme
                .cache()
                ;

        JavaPairRDD<String, Tuple2<String, String>> titleBasics = sc.textFile("title.basics/part-00000")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(l[1],l[2])))//identificador do filme, (nome do filme, ano)
                .cache()
                ;

        //---------------------------------------------topGenres----------------------------------------------

        List<Tuple2<Integer, Tuple2<String, Integer>>> topGenres = sc.textFile("title.basics/part-00000")
                .flatMapToPair(l -> {
                    String[] f = l.split("\t");
                    if (!f[0].equals("tconst") && !f[2].equals("\\N"))
                        return Arrays.stream(f[3].split(",")).map(g -> new Tuple2<>(round(Integer.parseInt(f[2])),g)).iterator();
                    else
                        return Collections.emptyIterator();
                })//(ano,genre)
                .groupByKey()//(ano,[genre, genre, genre...])
                .mapValues(objetivo2::countGenrePerDecade)//(ano,[(genre, genrecount),(ano,genrecount)...])
                .mapValues(objetivo2::getBestGenreDecade)//(ano,(genre,genrecount))
                .sortByKey()
                .collect()
                ;

        //---------------------------------------------seasonHits----------------------------------------------

        List<Tuple2<String, Tuple2<String, Float>>> seasonHits = titleBasics.join(filmRatings) //(tt...,((nome filme, ano),rating))
                .filter(l -> !l._2._1._1.equals("\\N") && !l._2._1._2.equals("\\N"))
                .mapToPair(p -> new Tuple2<>(Float.parseFloat(p._2._2),p._2._1)) //elimina o identificador do filme -> (rating,(nome filme,ano))
                .sortByKey(false) //ordena o rating descente
                .mapToPair(p -> new Tuple2<>(p._2._2, new Tuple2<>(p._2._1,p._1))) //(ano,(nome filme,rating))
                .groupByKey() // (ano,[(nome filme, rating),(nome filme, rating) ...])
                .sortByKey()
                .mapValues(l -> l.iterator().next()) // (ano,(nome filme, rating))
                .collect()
                ;

        //----------------------Top 10 atores que participam em mais titulos diferentes------------------------

        List<Tuple2<Tuple2<String, String>, Integer>> top10s = sc.textFile("title.principals/part-00000")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst") && !l[2].equals("\\N"))
                .mapToPair(l -> new Tuple2<>(l[2], 1))//(identificador do ator, 1)
                .foldByKey(0, (v1, v2) -> v1 + v2)//(identificador do ator(juntos), numero de filmes)
                .join(actorsNames) // (identificador do ator,(numero de filmes,nome ator))
                .mapToPair(l -> new Tuple2<>(l._2._1,new Tuple2<>(l._1,l._2._2)))//(numero de filmes,(identificador do ator,nome ator))
                .sortByKey(false, 1) // 1st arg configures ascending sort, 2nd arg configures one task
                .mapToPair(l -> new Tuple2<>(new Tuple2<>(l._2._1,l._2._2),l._1))//(identificador do ator,,nome ator), numero de filmes)
                .take(10)
        ;

        //---------------------------------------------Impressao-----------------------------------------------

        System.out.println("--------------------Season Hits por ano--------------------");
        topGenres.forEach(System.out::println);
        System.out.println("--------------------Titulo mais bem classificado por cada ano--------------------");
        seasonHits.forEach(System.out::println);
        System.out.println("--------------------Top 10 atores que participam em mais titulos diferentes--------------------");
        top10s.forEach(System.out::println);
    }
}
