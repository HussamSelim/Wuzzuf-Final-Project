import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MainClass {

	public static void main(String[] args) {
		Logger.getLogger ("org").setLevel (Level.ERROR);
		//Create a Spark conext
		SparkConf conf = new SparkConf().setAppName("Wuzzuf").setMaster("local[3]");
		JavaSparkContext context= new JavaSparkContext(conf);
		// LOAD DATASETS
		JavaRDD<String> WuzzufDataSet= context.textFile("E:\\iti\\Wuzzuf_Jobs.csv");
		//Transformation
		JavaRDD<String> WuzzufDataSetUpdated= WuzzufDataSet.distinct();
		
		//Transformation
		JavaRDD<String> jobs= WuzzufDataSetUpdated
			.map(MainClass::extractjobs)
			.filter(StringUtils::isNotBlank);
		
		
		JavaRDD<String> company= WuzzufDataSetUpdated
				.map(MainClass::extractcompanies)
				.filter(StringUtils::isNotBlank);
		
		JavaRDD<String> location= WuzzufDataSetUpdated
				.map(MainClass::extractlocation)
				.filter(StringUtils::isNotBlank);
		
		JavaRDD<String> skills= WuzzufDataSetUpdated
				.map(MainClass::extractskills)
				.filter(StringUtils::isNotBlank);

	}

	public static String extractjobs(String job) {
        try {
            return job.split (",")[0];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
	}
	public static String extractcompanies(String companies) {
        try {
            return companies.split (",")[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
	}
	public static String extractlocation(String location) {
        try {
            return location.split (",")[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
	}
	public static String extractskills(String skills) {
        try {
            return skills.split (",")[7];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
	}
}
