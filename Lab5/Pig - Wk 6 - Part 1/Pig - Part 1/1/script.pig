--load Dataset 
dataset = LOAD '/home/cloudera/Desktop/LAB5 Part 1/1/dataset/InputForWC.txt' using TextLoader AS (line:chararray);
 
--splite to word by word (FLATTEN) & TOKENIZE 
flattenWords = FOREACH dataset GENERATE FLATTEN(TOKENIZE(line,'\t|\' \'')) AS word;
 
--words count  

grouped = GROUP flattenWords BY word;
count = FOREACH grouped GENERATE group, COUNT($1);

-- display result:   
--DUMP count;

--STORE output 
 STORE count INTO '/home/cloudera/Desktop/LAB5 Part 1/1/output' using PigStorage('\t');
 

