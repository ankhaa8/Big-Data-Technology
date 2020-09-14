REGISTER /usr/lib/pig/piggybank.jar
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

moviesData = LOAD '/home/cloudera/Desktop/Labs/Lab5Part2/input/movies.csv' USING CSVLoader AS(movieId:int, title:chararray, genres:chararray);
ratings = LOAD '/home/cloudera/Desktop/Labs/Lab5Part2/input/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);

adventureMovies = FILTER moviesData BY (genres MATCHES '.*Adventure*.');
adventureMoviesIds = FOREACH adventureMovies GENERATE movieId, genres;
moviesJoinRating = JOIN ratings BY movieId, adventureMoviesIds BY movieId;

moviesANDRating = FOREACH moviesJoinRating GENERATE $1 AS movieId, $2 AS rating, $5 AS genres;
moviesHighest = FILTER moviesANDRating BY rating == 5;

distinctMovies = DISTINCT moviesHighest;
moviesSorted = ORDER distinctMovies BY movieId;
top20Movie = LIMIT moviesSorted 20;

moviesDetails = JOIN top20Movie BY movieId, adventureMovies BY movieId;
topAdventureMovies = FOREACH moviesDetails GENERATE top20Movie::movieId AS MovieId, top20Movie::genres AS Genres, top20Movie::rating AS Rating, title AS Title;

STORE topAdventureMovies INTO '/home/cloudera/Desktop/Labs/Lab5Part2/5/output' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','UNIX', 'WRITE_OUTPUT_HEADER');


