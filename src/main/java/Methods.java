
import java.awt.Color;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;
import smile.data.DataFrame;
import smile.data.measure.NominalScale;
import smile.io.Read;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author MIDO
 */
public class Methods {
    private DataFrame jobDataFrame;
    public DataFrame readCSV(String path) {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader ();
        DataFrame df = null;
        try {
            df = Read.csv (path, format);
//            System.out.println(df.summary ());
            df = df.select ("Title", "Company", "Location", "Type", "Level","YearsExp","Country","Skills");
            System.out.println(df.summary ());
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace ();
        }
        jobDataFrame = df;
        // System.out.println (df.summary ());
        return df;
    }
    public static int[] encodeCategory(DataFrame df, String columnName) {
        String[] values = df.stringVector (columnName).distinct ().toArray (new String[]{});
        int[] pclassValues = df.stringVector (columnName).factorize (new NominalScale (values)).toIntArray ();
        return pclassValues;
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
                return skills.split ("\"")[1];
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
