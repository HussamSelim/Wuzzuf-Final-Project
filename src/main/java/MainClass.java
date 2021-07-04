import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                DataFrame jobSM = obj.readCSV ("F:\\Iti\\Foundation period\\Java & UML programming\\Wuzzuf_Jobs.csv");
                System.in.read();
                System.out.println ("=======Data Structure=========");
                System.out.println (jobSM.structure ());
                System.in.read();
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
                JavaRDD<String> WuzzufDataSet= context.textFile("F:\\Iti\\Foundation period\\Java & UML programming\\Wuzzuf_Jobs.csv");
                //Transformation
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
                //Counting Skills
                JavaRDD<String> words = skills.flatMap (skill -> Arrays.asList (skill
                        .toLowerCase ()
                        .trim ()
                        .split (",")).iterator ());
                System.out.println(words.toString ());
                // COUNTING
                //                System.out.println(words.count());
                Map<String, Long> wordCounts = words.countByValue ();
                Map<String, Long> sortedSkills= wordCounts.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue)-> oldValue, LinkedHashMap::new));
                // DISPLAY
                System.out.println("Skill               : Frequancy of Skill    ");
                sortedSkills.forEach((k, v) -> System.out.println(k +"  :  "+v));
                System.in.read();
                //Counting Companies
                Map<String,Long> companiesCount= company.countByValue();
                Map<String, Long> sortedCompany= companiesCount.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue)-> oldValue, LinkedHashMap::new));
                System.out.println("Company               : Frequancy of Company    ");
                sortedCompany.forEach((k, v) -> System.out.println(k +"  :  "+v));
                                System.in.read();
                // Plotting the Pie chart for the 5 most publishing companies on Wuzzuf

                obj.pieChart(sortedCompany);
                                System.in.read();
                //Counting Jobs
                Map<String,Long> jobsCount= jobs.countByValue();
                Map<String, Long> sortedJobs= jobsCount.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue)-> oldValue, LinkedHashMap::new));
                ArrayList<String> jobKeys= new ArrayList<String>(sortedJobs.keySet());
                ArrayList<Long> jobValues= new ArrayList<Long>(sortedJobs.values());

                ArrayList<String> first8JobKeys= (ArrayList<String>) jobKeys.stream().limit(8).collect(Collectors.toList());
                ArrayList<Long> first8JobValues= (ArrayList<Long>) jobValues.stream().limit(8).collect(Collectors.toList());
                //Calling graphJobPopularity to plot the bar chart
                obj.graphJobPopularity(first8JobKeys, first8JobValues);
                System.in.read();
                System.out.println("Job               : Frequancy of job    ");
                sortedJobs.forEach((k, v) -> System.out.println(k +"  :  "+v));
                //jobValues.forEach(x->System.out.println(x));



                // Counting Locations
                //		Map<String,Long> locationCount= location.countByValue();
                //		Map<String, Long> sortedLocation= locationCount.entrySet().stream()
                //		.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                //		.collect(Collectors.toMap(
                //				Map.Entry::getKey,
                //				Map.Entry::getValue,
                //				(oldValue, newValue)-> oldValue, LinkedHashMap::new));
                //		ArrayList<String> locationKeys= new ArrayList<String>(sortedLocation.keySet());
                //		ArrayList<Long> locationValues= new ArrayList<Long>(sortedLocation.values());
                //		
                //		ArrayList<String> first8LocationKeys= (ArrayList<String>) locationKeys.stream().limit(8).collect(Collectors.toList());
                //		ArrayList<Long> first8LocationValues= (ArrayList<Long>) locationValues.stream().limit(8).collect(Collectors.toList());
                //		
                //		obj.graphPopularAreas(first8LocationKeys, first8LocationValues);




            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
    }
}

