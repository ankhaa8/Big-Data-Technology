
moviesData = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/movies.csv' USING PigStorage(',') AS(movieId:int, title:chararray, genres:chararray);
ratings = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);
adventureMovies = FILTER moviesData BY (genres MATCHES '.*Adventure*.');
adventureMoviesIds = FOREACH adventureMovies GENERATE movieId, genres;
moviesJoinRating = JOIN ratings BY movieId, adventureMoviesIds BY movieId;

moviesANDRating = FOREACH moviesJoinRating GENERATE $1 AS movieId, $2 AS rating, $5 AS genres;
moviesHighest = FILTER moviesANDRating BY rating == 5;
distinctMovies = DISTINCT moviesHighest;
moviesSorted = ORDER distinctMovies BY movieId;
top20Movie = LIMIT moviesSorted 20;
moviesDetails = JOIN top20Movie BY movieId, adventureMovies BY movieId;
topAdventureMovies = FOREACH moviesDetails GENERATE top20Movie::movieId, top20Movie::genres, top20Movie::rating, title;

dump topAdventureMovies;

