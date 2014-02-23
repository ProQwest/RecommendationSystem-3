import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.dao.PrefetchingUserEventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.history.UserHistory;
import org.grouplens.lenskit.data.pref.PreferenceDomain;
import org.grouplens.lenskit.data.pref.PreferenceDomainBuilder;
import org.grouplens.lenskit.eval.data.CSVDataSource;
import org.grouplens.lenskit.eval.data.CSVDataSourceBuilder;
import org.grouplens.lenskit.collections.LongUtils;
import org.grouplens.lenskit.core.*;
import org.grouplens.lenskit.cursors.*;


class NonPersRecommender {
	
	int movies[];
    File file;
    CSVDataSource datasource;
    HashMap<Long,ArrayList<Long>> movietoUseMap;
    HashMap<Long,Double> simpleIntersectionResults;
    HashMap<Long,Double> simpleResultsSorted;
    
    HashMap<Long,Double> advIntersectionResults;
    HashMap<Long,Double> advResultsSorted;
    
    public NonPersRecommender(File file) {
        movies = new int[5];
        this.file = file;
        movietoUseMap= new HashMap<Long,ArrayList<Long>>();
        simpleIntersectionResults = new HashMap<Long,Double>();
        simpleResultsSorted = new HashMap<Long,Double>();
        
        advIntersectionResults = new HashMap<Long,Double>();
        advResultsSorted = new HashMap<Long,Double>();
    }
    
    public void getData() {
        
        //Build preference domain
        PreferenceDomain prefdom = new PreferenceDomainBuilder(1, 5).
                setPrecision(1).
                build();
        //Get the into the data source
        datasource = new CSVDataSourceBuilder(file).
                setName("nonpers").
                setDomain(prefdom).
                build();
    }
    
    public void eventExample() {
        
        EventDAO eventdao = datasource.getEventDAO();
        Cursor<Event> mycursor = eventdao.streamEvents();
        
        System.out.println("Events:");
        for(Event oneevent : mycursor) {
            System.out.println(oneevent.getItemId() +  ", " + oneevent.getUserId());
        }
    }
    
    public void buildMap()
    {
    	EventDAO eventdao = datasource.getEventDAO();
        Cursor<Event> mycursor = eventdao.streamEvents();
        
        System.out.println("Events:");
        for(Event oneevent : mycursor) {
        	Long movieId = oneevent.getItemId();
            if(movietoUseMap.containsKey(movieId))
            {
            	ArrayList<Long> tempArray = movietoUseMap.get(movieId);
            	tempArray.add(oneevent.getUserId());
            	movietoUseMap.put(movieId, tempArray);
            }
            else
            {
            	ArrayList<Long> tempArray = new ArrayList<Long>();
            	tempArray.add(oneevent.getUserId());
            	movietoUseMap.put(movieId,tempArray);
            }
        }
        
//        System.out.println("Map Results");
//        for(Long l: movietoUseMap.keySet())
//        {
//        	System.out.println(l + ","+ movietoUseMap.get(l));
//        	for(Long items: movietoUseMap.get(l))
//        	{
//        		System.out.print(items + ",");
//        	}
//        	System.out.println();
//        }
    }
    
    public void SimpleIntersection()
    {
    	long x = 671;
    	ArrayList<Long> xlist = movietoUseMap.get(x);
    	int xCount = xlist.size();
    	
    	//Intersection (A&B)
    	for(Long itemList: movietoUseMap.keySet())
    	{
    		double result=0;
    		if(itemList!=x)
    		{
	    		ArrayList<Long> ylist = movietoUseMap.get(itemList);
	    		result = calculateIntersection(xlist,ylist);
	    		
    		}
    		//calculate (x & y)/x
    		double simpleResult = result/xCount;
    		simpleIntersectionResults.put(itemList,simpleResult);
    	}
    	
    	// sort the array in descending order
    	List<Double> mapValues = new ArrayList<Double>(simpleIntersectionResults.values());
    	List<Double> originalmapValues = new ArrayList<Double>(mapValues);
     	List<Long> mapKeys = new ArrayList<Long>(simpleIntersectionResults.keySet());
    	Collections.sort(mapValues, Collections.reverseOrder());
    	
    	
    	System.out.println("Top 5 results:Simple");
    	for(int count=0; count<5; count++)
    	{
    		//System.out.println(mapValues.get(count));
    		simpleResultsSorted.put(mapKeys.get(originalmapValues.indexOf(mapValues.get(count))), mapValues.get(count));
    	}
    	for(Long items : simpleResultsSorted.keySet())
    	{
    		System.out.println(items + "," + roundTwoDecimals(simpleResultsSorted.get(items)));
    	}
    	
    }

    
    public void AdvanceIntersection()
    {
    	//first calculate !x(all the users who didn't watch x)
    	long x=671;
    	Set<Long> notXList = new TreeSet<Long>();
    	for(Long itemList: movietoUseMap.keySet())
    	{
    		if(itemList!=x)
    		{
    			ArrayList<Long> yList = movietoUseMap.get(itemList);
    			// Using TreeSet as the number of entries for !x will be huge	   
    			for(long values: yList)
    			{
    				
    				notXList.add(values);
    			}
    		}
    	}
    	for(long temp: movietoUseMap.get(x))
		{
    		if(notXList.contains(temp))
    			notXList.remove(temp);
		}
    	
    	
    	for(Long itemList: movietoUseMap.keySet())
    	{
    		double result=0;
    		if(itemList!=x)
    		{
    			ArrayList<Long> yList = movietoUseMap.get(itemList);

    			// Calculate ((x and y) / x) / ((!x and y) / !x)
    			// 1. Calculate  ((x and y) / x)
    			// 2. Calculate ((!x and y) / !x)
    			//1. The Numerator is already computed so all we need to do is just go through the simpleIntersectionResults
    			double numerator = simpleIntersectionResults.get(itemList);
    			//2. The Denominator needs to be calculated
    			List<Long> notXListAll = new ArrayList<Long>(notXList);
    			result = calculateIntersection(notXListAll,yList); // (!x&y)
    			
    			double denom = result/notXListAll.size();
    			double finalResult = numerator/denom;
    			advIntersectionResults.put(itemList, finalResult);

    		} 
    	}
	
    	// Sorting to get the Top results
    	// sort the array in descending order
    	List<Double> mapValues = new ArrayList<Double>(advIntersectionResults.values());
    	List<Double> originalmapValues = new ArrayList<Double>(mapValues);
    	List<Long> mapKeys = new ArrayList<Long>(advIntersectionResults.keySet());
    	Collections.sort(mapValues, Collections.reverseOrder());

    	for(int count=0; count<5; count++)
    	{
    		//System.out.println(mapValues.get(count));
    		advResultsSorted.put(mapKeys.get(originalmapValues.indexOf(mapValues.get(count))), mapValues.get(count));
    	}
    	System.out.println("Top 5 Results: Advanced");
    	for(Long items : advResultsSorted.keySet())
    	{
    		System.out.println(items + "," + roundTwoDecimals(advResultsSorted.get(items)));
    	}
    }
    
    
    double roundTwoDecimals(double d) {
        DecimalFormat twoDForm = new DecimalFormat("#.##");
    return Double.valueOf(twoDForm.format(d));
}
    
//    280
//    671
//    105
   public Double calculateIntersection(List<Long> array1, List<Long> array2) {
    
        LongSet set1 = new LongArraySet(array1);
        LongSet set2 = new LongArraySet(array2);
        LongSortedSet set3, set4;
        
//        System.out.println("Set 1: " + set1.toString());
//        System.out.println("Set 2: " + set2.toString());
        
        set3 = LongUtils.setDifference(set1, set2);
        set4 = LongUtils.setDifference(set2, set1);
        
//        System.out.println(set3.toString());
//        System.out.println(set4.toString());
        
        for(Long item : set3) {
            if(set1.contains(item))
                set1.remove(item);
        }
        
        for(Long item : set4) {
            if(set1.contains(item)) {
                set1.remove(item);
            }
        }
        return (double) set1.size();
        
 
    }
	
	
	
    public static void main(String[] args) {
	// Movies array contains the movie IDs of the top 5 movies.
	int movies[] = new int[5];
	NonPersRecommender rec = new NonPersRecommender(new File("/Applications/RecommenderSystem/data/recsys-data-ratings.csv"));
	rec.getData();
	//rec.eventExample();
	rec.buildMap();
	//rec.longSetIntersectionExample();
	rec.SimpleIntersection();
	rec.AdvanceIntersection();
//	ArrayList<Long> array1 = new ArrayList<Long>();
//	ArrayList<Long> array2 = new ArrayList<Long>();
//	array1.add((long)1);
//	array1.add((long)2);
//	array2.add((long)2);
//	array2.add((long)3);
//	//rec.calculateIntersection(array1, array2);
	
	// Write the top 5 movies, one per line, to a text file.
	try {
	    PrintWriter writer = new PrintWriter("/Applications/RecommenderSystem/data/pa1-result.txt","UTF-8");
       
	    for (int movieId : movies) {
		writer.println(movieId);
	    }

	    writer.close();
	    
	} catch (Exception e) {
	    System.out.println(e.getMessage());
	}
    }
}