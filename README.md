# An CF-based Recommendation System
**DataSet** : netflix competition dataset
**Algorithm** : Item_based Collaborative Filtering

MapReduce phases:
1. ItemIndexedData
input: movie, user, rating
output: key = movie, value = user:rating:avg:div  (avg is the averaged rating of a movie, and div is sqrt of variance)

2. UserIndexedData
input: from ItemIndexedData output
output: key = movie1, value = movie2: (r1-avg1)*(r2-avg2)/div1/div2   (the cosine distance of movie1 and movie2, contributed by user_j)

3. PreSimilarity
input: from UserIndexedData output
output: key = movie1, value = {sim:movie2:s12, sim:movie3:s13,....}

4. Multiplication
input: from PreSimilarity output and ItemIndexedData output
output:  key = user:movie, value = rate  (contribution from similar item movie_j)

5. Sum
input: from Multiplication output
output: key = user:movie, value = rate (final prediciton of user's rating to this movie)











