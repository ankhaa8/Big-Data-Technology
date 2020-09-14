mysql -u root -p
--Enter password as cloudera

use cs523;
create table stocks (id int not null primary key,
 symbol varchar(4),
 quote_date TIMESTAMP ,
 open_price  FLOAT,
 high_price  FLOAT,
 low_price  FLOAT
 );
  
insert into stocks values (1, "AAPL", "2009-01-02",85.88,91.04,85.16),(2, "AAPL", "2008-01-02",199.27,200.26,192.55),(3, "AAPL", "2007-01-03",86.29,86.58,81.9);

select * from stocks;
quit;


--import
sqoop import --connect jdbc:mysql://localhost/cs523 --username root -password cloudera --table stocks --columns "id,symbol,open_price" --target-dir=/user/cloudera/sqoopOutputStocks -m 1 --fields-terminated-by '\t'

