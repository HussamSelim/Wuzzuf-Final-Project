import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import smile.data.DataFrame;
import smile.data.vector.IntVector;


public class MainClass {

	public static void main(String[] args) {
            
            
            try {
                // Creating dummy object to call functions from the Methods Class
                Methods obj = new Methods();
                //Using SMILE to print summary for the data
                System.out.println ("=======Data Summary=========");
                DataFrame jobSM = obj.readCSV ("src/main/resources/Wuzzuf_Jobs.csv");
                System.in.read();
                System.out.println ("=======Data Structure=========");
                System.out.println (jobSM.structure ());
                System.in.read();
                // Enocoding YearsExp column
                jobSM = jobSM.merge (IntVector.of ("YearsExpValues",
                        obj.encodeCategory (jobSM, "YearsExp")));
                
                System.out.println ("=======Encoding YearsExp column Data==============");
                System.out.println (jobSM.structure ());
                System.out.println (jobSM);
                System.in.read();
                //Create a Spark conext
                Logger.getLogger ("org").setLevel (Level.ERROR);
                SparkConf conf = new SparkConf().setAppName("Jobs").setMaster("local[3]");
                JavaSparkContext context= new JavaSparkContext(conf);
                // LOAD DATASETS
                JavaRDD<String> WuzzufDataSet= context.textFile("src/main/resources/Wuzzuf_Jobs.csv");
                
                //Removing Nulls 
                DataFrame newJobSM = Methods.processData(jobSM);
                System.out.println ("=======Cleaned Data==============");
                System.out.println (jobSM);
                System.in.read();
                 
                //Transformation and removing duplicates
                JavaRDD<String> WuzzufDataSetUpdated= WuzzufDataSet.distinct();
                //Transformation
                JavaRDD<String> jobs= WuzzufDataSetUpdated
                        .map(Methods::extractjobs)
                        .filter(StringUtils::isNotBlank);


                JavaRDD<String> company= WuzzufDataSetUpdated
                        .map(Methods::extractcompanies)
                        .filter(StringUtils::isNotBlank);

                JavaRDD<String> location= WuzzufDataSetUpdated
                        .map(Methods::extractlocation)
                        .filter(StringUtils::isNotBlank);

                JavaRDD<String> skills= WuzzufDataSetUpdated
                        .map(Methods::extractskills)
                        .filter(StringUtils::isNotBlank);
                Map<String, Long> sortedSkills = Methods.countSkills(skills);
                // DISPLAY
                System.out.println("Skill               : Frequancy of Skill    ");
                Methods.printNValues(sortedSkills,10);
//                sortedSkills.forEach((k, v) -> System.out.println(k +"  :  "+v));
                System.in.read();
                //Counting Companies
                Map<String, Long> sortedCompany = Methods.countRows(company);
                System.out.println("Company               : Frequancy of Company    ");
                // Display top 10 Companies
                Methods.printNValues(sortedCompany,10);
                System.in.read();
                // Plotting the Pie chart for the 5 most publishing companies on Wuzzuf
                obj.pieChart(sortedCompany);
                System.in.read();
                //Counting Jobs
                Map<String, Long> sortedJobs= Methods.countRows(jobs);
                ArrayList<String> jobKeys= new ArrayList<String>(sortedJobs.keySet());
                ArrayList<Long> jobValues= new ArrayList<Long>(sortedJobs.values());

                ArrayList<String> first8JobKeys= (ArrayList<String>) jobKeys.stream().limit(8).collect(Collectors.toList());
                ArrayList<Long> first8JobValues= (ArrayList<Long>) jobValues.stream().limit(8).collect(Collectors.toList());
                //Calling graphJobPopularity to plot the bar chart
                obj.graphJobPopularity(first8JobKeys, first8JobValues);
                System.in.read();
                // Display top 10 Jobs
                System.out.println("Job               : Frequancy of job    ");
                Methods.printNValues(sortedJobs, 10);
                System.in.read();
                //jobValues.forEach(x->System.out.println(x));



                // Counting Locations
                Map<String, Long> sortedLocation= Methods.countRows(location);
                ArrayList<String> locationKeys= new ArrayList<String>(sortedLocation.keySet());
                ArrayList<Long> locationValues= new ArrayList<Long>(sortedLocation.values());

                ArrayList<String> first8LocationKeys= (ArrayList<String>) locationKeys.stream().limit(8).collect(Collectors.toList());
                ArrayList<Long> first8LocationValues= (ArrayList<Long>) locationValues.stream().limit(8).collect(Collectors.toList());

                obj.graphPopularAreas(first8LocationKeys, first8LocationValues);
                System.in.read();
                Methods.printNValues(sortedLocation, 10);
                System.in.read();



            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
    }
}

