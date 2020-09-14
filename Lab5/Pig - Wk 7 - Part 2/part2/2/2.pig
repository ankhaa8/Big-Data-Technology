rawdata = LOAD '/home/cloudera/Desktop/lab5Pig/Part2/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
filtered = FILTER rawdata BY (occupation=='lawyer') AND (gender=='M');
ordered = ORDER filtered BY age DESC;
top = LIMIT ordered 1;
userid = FOREACH top GENERATE userId;

dump userid;

