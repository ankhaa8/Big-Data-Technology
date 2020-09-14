--load Dataset
users = LOAD '/home/cloudera/Desktop/LAB5 Part 1/2/dataset/users.csv' USING PigStorage(',') AS (userName:chararray, age:int);
pages = LOAD '/home/cloudera/Desktop/LAB5 Part 1/2/dataset/pages.csv' USING PigStorage(',') AS (userName:chararray, siteName:chararray);

--filter users aged between 18 - 25
usersFiltered = FILTER users BY  age >= 18 AND  age <= 25;

-- relationship between two dataset
userspages = JOIN usersFiltered BY userName,pages BY userName;
 
--Filter Find the top 5 most visited sites.
groupedRecords = GROUP userspages BY siteName;
 
counted = FOREACH groupedRecords GENERATE group AS siteName,COUNT(userspages.siteName) as count;
result = FOREACH counted GENERATE siteName, count;

orderByCount = ORDER result BY count DESC;

limitRec = LIMIT orderByCount 5;
 
-- display result:  
DUMP limitRec;

 
