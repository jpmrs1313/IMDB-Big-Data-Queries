import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class objetivo1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("g4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //final String fileDir = "file:////home/mcmaia/GGCD/dados/mini/";
        final String fileDir  = "file:////home/mcmaia/Downloads/";
        final String extensao = "gz";

        sc.textFile(fileDir + "name.basics.tsv." + extensao)
                .map(l -> l.split("\t"))
                .map(l -> l[0] + "\t" + l[1] + "\t" + l[2] + "\t" + l[3] + "\t" + l[4])
                .saveAsTextFile("name.basics");

        sc.textFile( fileDir + "title.principals.tsv." + extensao)
                .saveAsTextFile("title.principals");

        sc.textFile(fileDir + "title.ratings.tsv." + extensao)
                .map(l -> l.split("\t"))
                .map(l -> l[0] + "\t" + l[1] + "\t" + l[2])
                .saveAsTextFile("title.ratings");

        sc.textFile(fileDir + "title.basics.tsv." + extensao)
                .map(l -> l.split("\t"))
                .map(l -> l[0] + "\t" + l[3] + "\t" + l[5] + "\t" + l[8])
                .saveAsTextFile("title.basics");
    }
}
