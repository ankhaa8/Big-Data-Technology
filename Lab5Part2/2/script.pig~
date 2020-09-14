data = LOAD '/home/cloudera/Desktop/Labs/Lab5Part2/input/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
filtered = FILTER data BY (occupation=='lawyer') AND (gender=='M');
ordered = ORDER filtered BY age DESC;
top = LIMIT ordered 1;
userid = FOREACH top GENERATE userId;
STORE userid INTO '/home/cloudera/Desktop/Labs/Lab5Part2/2/output' using PigStorage('\t');
dump userid;


