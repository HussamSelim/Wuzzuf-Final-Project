
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Rady
 */
public class Job_DAO extends Thread {
    public final List<Job> c_list;
    BufferedReader br;
	String line; 
        
    public Job_DAO() {
    	c_list=new ArrayList<Job>();
    	try {
			this.br = new BufferedReader(new FileReader("C:\\Users\\Rady\\Downloads\\Compressed\\Wuzzuf_Jobs.csv"));
			do{   
	            line=br.readLine(); 
	            if(line!=null){
	            	String [] parts=line.split(",");
	            	c_list.add(new Job(parts[0], parts[2], parts[1], parts[3], parts[4], parts[5], parts[6], parts[7]));
	            	}
	        }while(line!=null); 
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
