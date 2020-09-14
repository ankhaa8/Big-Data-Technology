REGISTER /usr/lib/pig/piggybank.jar
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

movieData = LOAD '/home/cloudera/Desktop/Labs/Lab5Part2/input/movies.csv' USING CSVLoader AS(movieId:int, title:chararray, genres:chararray);
filtered = FILTER movieData BY STARTSWITH(title, 'a') OR STARTSWITH(title, 'A');
genreList = FOREACH filtered GENERATE STRSPLIT(genres, '\\|', 0);
flat = FOREACH genreList GENERATE FLATTEN($0);
tokens = FOREACH flat GENERATE $0, 1;
grouped = GROUP tokens BY $0;
summed = FOREACH grouped GENERATE $0, COUNT($1);
sorted = ORDER summed BY $0;
STORE sorted INTO '/home/cloudera/Desktop/Labs/Lab5Part2/3/output' using PigStorage('\t');
dump sorted;


