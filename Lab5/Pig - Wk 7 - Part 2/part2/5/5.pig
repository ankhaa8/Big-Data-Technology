moviesData = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/movies.csv' USING PigStorage(',') AS(movieId:int, title:chararray, genres:chararray);
ratings = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);
users = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);

adventureMovies = FILTER moviesData BY (genres MATCHES '.*Adventure*.');
adventureMoviesIds = FOREACH adventureMovies GENERATE movieId, genres;
moviesJoinRating = JOIN ratings BY movieId, adventureMoviesIds BY movieId;
moviesANDRating = FOREACH moviesJoinRating GENERATE $1 AS movieId, $2 AS rating, $5 AS genres;
moviesHighest = FILTER moviesANDRating BY rating == 5;
moviesSorted = ORDER moviesHighest BY movieId;
top20Movie = LIMIT moviesSorted 20;

topMoviesRating = JOIN top20Movie BY movieId, ratings BY movieId;
usersIds = FOREACH topMoviesRating GENERATE userId;
distinctUserIds = DISTINCT usersIds;
userDetails = JOIN distinctUserIds By userId, users BY userId;
programmerMaleUsers = FILTER userDetails BY gender == 'M' AND occupation == 'programmer';

groupd = GROUP programmerMaleUsers BY users::userId;
usrId = FOREACH groupd GENERATE group;
groupall = GROUP usrId ALL;
result = FOREACH groupall GENERATE COUNT($1);

dump result;