package edu.umn.cs.recsys.uu;
import java.util.*;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.grouplens.lenskit.basic.AbstractItemScorer;
import org.grouplens.lenskit.data.dao.ItemEventDAO;
import org.grouplens.lenskit.data.dao.UserEventDAO;
import org.grouplens.lenskit.data.event.Rating;
import org.grouplens.lenskit.data.history.History;
import org.grouplens.lenskit.data.history.RatingVectorUserHistorySummarizer;
import org.grouplens.lenskit.data.history.UserHistory;
import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.VectorEntry;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;

import javax.annotation.Nonnull;
import javax.inject.Inject;

/**
 * User-user item scorer.
 * @author <a href="http://www.grouplens.org">GroupLens Research</a>
 */
public class SimpleUserUserItemScorer extends AbstractItemScorer {
    private final UserEventDAO userDao;
    private final ItemEventDAO itemDao;

    @Inject
    public SimpleUserUserItemScorer(UserEventDAO udao, ItemEventDAO idao) {
        userDao = udao;
        itemDao = idao;
    }

    @Override
    public void score(long user, @Nonnull MutableSparseVector scores) {
        SparseVector userVector = getUserRatingVector(user);
        double userVectorMean = userVector.mean();
        MutableSparseVector userMovieRatingMeanVector = userVector.mutableCopy();
        userMovieRatingMeanVector.add(-userVectorMean);
        
        // TODO Score items for this user using user-user collaborative filtering

        // This is the loop structure to iterate over items to score
        for (VectorEntry e: scores.fast(VectorEntry.State.EITHER)) {

            // 1. Users who rated the item
            LongSet possibleNeighbors = itemDao.getUsersForItem(e.getKey());
            possibleNeighbors.remove(user);
            
           //Set<Long> neighborRating = new HashSet<Long>();
            // 2. Calculate the mean centered rating for the users
            Map<Long,Double> similarityMap = new HashMap<Long,Double>();
            for(Long neighUser: possibleNeighbors)
            {
            	SparseVector neighborMovieRatingVector = getUserRatingVector(neighUser);
            	double neighbormean = neighborMovieRatingVector.mean();
            	MutableSparseVector neighMovieRatingMeanVector = neighborMovieRatingVector.mutableCopy();
            	neighMovieRatingMeanVector.add(-neighbormean);
            	
            	double similarityUserNeighbor = new CosineVectorSimilarity().similarity(userMovieRatingMeanVector,
            			neighMovieRatingMeanVector);
            	similarityMap.put(neighUser, similarityUserNeighbor);
            }
            // sort on value to pick up the first 30
            Map<Long,Double> similarityMapSorted = sortByValue(similarityMap);
            Map<Long,Double> similarity30 = new LinkedHashMap<Long,Double>();
            int count=0;
            for(Long user30: similarityMapSorted.keySet())
            {
            	if(count<30)
            	{
            		similarity30.put(user30, similarityMapSorted.get(user30));
            		count+=1;
            	}	
            }
            double sumRating =0.0;
            double sumSimilarities =0.0;
            for(Long eachUser: similarity30.keySet())
            {
            	SparseVector neighbor30MovieRatingVector = getUserRatingVector(eachUser);
            	double neighbor30mean = neighbor30MovieRatingVector.mean();
            	// for each user get the rating for specific item and subtract the mean from it
            	double eachNeighItemRating = getUserRatingVector(eachUser).get(e.getKey());
            	double eachNeighnormalizedRating = eachNeighItemRating - neighbor30mean;
            	double sim = similarity30.get(eachUser);
            	sumRating+=sim*eachNeighnormalizedRating;
            	sumSimilarities+=sim;
            }
            double predictedResult = userVectorMean +  (sumRating/sumSimilarities); 
            scores.set(e.getKey(),predictedResult);
        }
    }

    public static Map<Long, Double> sortByValue(Map<Long, Double> map) {
        List<Map.Entry<Long, Double>> list = new LinkedList<Map.Entry<Long, Double>>(map.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<Long, Double>>() {

            public int compare(Map.Entry<Long, Double> m1, Map.Entry<Long, Double> m2) {
                return (m2.getValue()).compareTo(m1.getValue());
            }
        });

        Map<Long, Double> result = new LinkedHashMap<Long, Double>();
        for (Map.Entry<Long, Double> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    
    /**
     * Get a user's rating vector.
     * @param user The user ID.
     * @return The rating vector.
     */
    private SparseVector getUserRatingVector(long user) {
        UserHistory<Rating> history = userDao.getEventsForUser(user, Rating.class);
        if (history == null) {
            history = History.forUser(user);
        }
        return RatingVectorUserHistorySummarizer.makeRatingVector(history);
    }
}
