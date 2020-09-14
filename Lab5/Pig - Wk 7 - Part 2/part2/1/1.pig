movieData = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
filtered = FILTER movieData BY (occupation=='lawyer') AND (gender=='M');
grouped = GROUP filtered ALL;
count = FOREACH grouped GENERATE COUNT($1);
dump count;
