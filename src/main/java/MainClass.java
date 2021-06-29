import java.awt.Color;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;


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

		
		//counting Companies
		Map<String,Long> companiesCount= company.countByValue();
		Map<String, Long> sortedCompany= companiesCount.entrySet().stream()
		.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
		.collect(Collectors.toMap(
				Map.Entry::getKey,
				Map.Entry::getValue,
				(oldValue, newValue)-> oldValue, LinkedHashMap::new));
//		for(Map.Entry<String, Long>entry:sortedCompany.entrySet()) { System.out.println(entry.getKey()+':'+entry.getValue());}	
		
		// Plotting the Pie chart for the 5 most publishing companies on Wuzzuf
		MainClass obj= new MainClass();
//		obj.pieChart(sortedCompany);
		
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
//		obj.graphJobPopularity(first8JobKeys, first8JobValues);
//		jobKeys.forEach(x->System.out.println(x));
//		jobValues.forEach(x->System.out.println(x));
		
		
		
		// Counting Locations
		Map<String,Long> locationCount= location.countByValue();
		Map<String, Long> sortedLocation= locationCount.entrySet().stream()
		.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
		.collect(Collectors.toMap(
				Map.Entry::getKey,
				Map.Entry::getValue,
				(oldValue, newValue)-> oldValue, LinkedHashMap::new));
		ArrayList<String> locationKeys= new ArrayList<String>(sortedLocation.keySet());
		ArrayList<Long> locationValues= new ArrayList<Long>(sortedLocation.values());
		
		ArrayList<String> first8LocationKeys= (ArrayList<String>) locationKeys.stream().limit(8).collect(Collectors.toList());
		ArrayList<Long> first8LocationValues= (ArrayList<Long>) locationValues.stream().limit(8).collect(Collectors.toList());
		
		obj.graphPopularAreas(first8LocationKeys, first8LocationValues);
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
            return "";}
        }
        
        public void pieChart(Map result) {
        PieChart chart = new PieChartBuilder().width (800).height (600).title (getClass().getSimpleName()).build ();
        // Customize Chart
        Color[] sliceColors= new Color[]{new Color (180, 68, 50), new Color (130, 105, 120), new Color (80, 143, 160)};
        chart.getStyler().setSeriesColors(sliceColors);
        
        chart.addSeries((String) result.keySet().toArray()[0], (Number) result.values().toArray()[0]);
    	chart.addSeries((String) result.keySet().toArray()[1], (Number) result.values().toArray()[1]);
    	chart.addSeries((String) result.keySet().toArray()[2], (Number) result.values().toArray()[2]);
    	chart.addSeries((String) result.keySet().toArray()[3], (Number) result.values().toArray()[3]);
    	chart.addSeries((String) result.keySet().toArray()[4], (Number) result.values().toArray()[4]);
    	new SwingWrapper(chart).displayChart();
    	   
       }
        public static void graphJobPopularity(List<String> jobKeys, List<Long> jobValues ) {
        CategoryChart chart = new CategoryChartBuilder().width (1024).height (768).title ("Most Popular Job Titles").xAxisTitle("Jobs").yAxisTitle("Popularity").build();
		chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
		chart.getStyler().setHasAnnotations(true);
		chart.getStyler().setStacked(true);
		chart.addSeries("Jobs Popularity",jobKeys, jobValues);
		
		new SwingWrapper(chart).displayChart();
        }
        
        public static void graphPopularAreas(List<String> locationKeys, List<Long> locationValues ) {
            CategoryChart chart = new CategoryChartBuilder().width (1024).height (768).title ("Most Popular Job Titles").xAxisTitle("Jobs").yAxisTitle("Popularity").build();
    		chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
    		chart.getStyler().setHasAnnotations(true);
    		chart.getStyler().setStacked(true);
    		chart.addSeries("Jobs Popularity",locationKeys, locationValues);
    		
    		new SwingWrapper(chart).displayChart();
            }
	}

