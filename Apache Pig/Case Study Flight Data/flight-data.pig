-- Case Study on Flight Data

pig -x local

-- Loading file 

flight_data = LOAD 'flights_details.csv' USING PigStorage(',');
dump flight_data;

-- Problem Statement- 1: Find out the top 5 most visited destinations. 

gen_flight_data = FOREACH flight_data GENERATE (int) $1 as year, (int)$10 as flight_num, (chararray)$17 as origin, (chararray)$18 as dest;
filter_null_values = FILTER gen_flight_data by dest is not null;
dump filter_null_values;
grp_dest = GROUP filter_null_values by dest;
dump grp_dest;
gen_count_dest = FOREACH grp_dest GENERATE group, count(filter_null_values.dest);
dump gen_count_dest;
order_count_desc = ORDER gen_count_dest by $1 desc;
limit_dest = LIMIT order_count_desc 5;
dump limit_dest;

airport_data = LOAD 'airports.csv' USING PigStorage(',');
dump airport_data;
gen_airport_data = FOREACH airport_data GENERATE (chararray)$0 as dest, (chararray)$2 as city, (chararray)$4 as country;
dump gen_airport_data;
joined_data = JOIN limit_dest by $0, gen_airport_data by dest;
count_desc = order joined_data by $1 DESC;
data= FOREACH count_desc GENERATE $0,$1,$3,$4;
dump data;

-- Problem Statement- 2: Which month has seen the most number of cancellations due to bad weather?

flight_data = LOAD 'flights_details.csv' USING PigStorage(',');
dump flight_data;
gen_flight_data_1 = FOREACH flight_data GENERATE (int)$2 as month, (int)$10 as flight_num, (int)$22 as cancelled , (chararray)$23 as cancel_code;
dump gen_flight_data_1;
fltr_data = FILTER gen_flight_data_1 by cancelled == 1 AND cancel_code == 'B';
dump fltr_data;
grp_month = GROUP fltr_data by month;
gen_grp =FOREACH grp_month GENERATE group, COUNT(fltr_data.cancelled);
dump gen_grp;

-- Problem Statement- 3: Top ten origins with the highest AVG departure delay  

flight_data = LOAD 'flights_details.csv' USING PigStorage(',');
dump flight_data;
gen_flight_data_2 = FOREACH flight_data GENERATE (int)$16 as dep_delay, (chararray)$17 as origin;
dump gen_flight_data_2;
flt = FILTER gen_flight_data_2 by (dep_delay is not null)  AND (origin is not null);
dump flt;
grp = GROUP flt by origin;
avg_delay = FOREACH grp GENERATE group, AVG(flt.dep_delay);
dump avg_delay;
result = ORDER avg_delay by $1 DESC;
top_ten = LIMIT result 10;
dump top_ten;

lookup = LOAD 'airports.csv' USING PigStorage(',');
dump lookup;
lookup_1 = FOREACH lookup GENERATE (chararray)$0 as origin, (chararray)$2 as city, (chararray)$4 as country;
dump lookup_1;
joined = JOIN lookup_1 by origin, top_ten by $0;
final = OFREACH joined GENERATE $0,$1,$2,$4;
dump final;
final_result = ORDER final by $3 DESC;
dump final_result;

-- Problem Statement- 4: Which route (origin & destination) has seen the maximum diversion? 

flight_data = LOAD 'flights_details.csv' USING PigStorage(',');
dump flight_data;
gen_flight_data_3 = FOREACH flight_data GENERATE (chararray)$17 as origin, (chararray)$18 as dest, (int)$24 as diversion;
dump gen_flight_data_3;
flt_1 = FILTER gen_flight_data_3 by (origin is not null) AND (dest is not null) AND (diversion==1);
dump flt_1;
grp_1 = GROUP flt_1 by (origin,dest);
count_div = FOREACH grp_1 GENERATE group, COUNT(flt_1.diversion);
order_desc = ORDER count_div by $1 DESC;
result_1 = LIMIT order_desc 10;
dump result_1;