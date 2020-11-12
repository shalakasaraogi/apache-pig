-- Case Study on IPL-T20 dataset
pig -x local
-- Loading the Data in  HDFS
ipl = LOAD 'ipl.xls' USING PigStorage(',') AS (ball:chararray, team:int, overs: float, teamBating:chararray, batsmen1:chararray, batsmen2:chararray, bowler:chararray,runs:int, other:int, result: chararray, playerOut:chararray);
DUMP ipl;

-- Problem Statement- 1: How many runs did David Warner score in the match
filterDavid = FILTER ipl BY batsmen1 MATCHES '^DA.+';
runsByDavid = FOREACH (GROUP filterDavid BY 'DA WARNER') GENERATE SUM(filterDavid.runs);
DUMP runsByDavid;

-- Problem Statement- 2: How many wickets did DJ Bravo take in match
wicketsByBravo = FILTER ipl BY bowler == 'DJ Bravo' AND result != 'run out';
bravoWicketsCount = FOREACH (GROUP wicketsByBravo ALL) GENERATE COUNT(wicketsByBravo);
DUMP bravoWicketsCount;

-- Problem Statement- 3: How many runs did Praveen Kumar concede in the match
praveenRunsFilter = FILTER ipl BY bowler == 'P Kumar' AND runs != 0;
praveenRunsCount = FOREACH (GROUP praveenRunsFilter ALL) GENERATE SUM(praveenRunsFilter.runs);
DUMP praveenRunsCount;

-- Problem Statement- 4: How many extras were bowled in the match
extraBowls = FILTER ipl BY other == 1;
extraBowlsCount = FOREACH (GROUP extraBowls ALL) GENERATE COUNT(extraBowls);
DUMP extraBowlsCount;

-- Problem Statement- 5: What is the final score of Gujarat Lions
lionsFilter = FILTER ipl BY teamBating == 'Gujarat Lions';
lionsScore = FOREACH (GROUP lionsFilter BY 'Gujarat Lions') GENERATE SUM(lionsFilter.runs);
DUMP lionsScore;

-- Problem Statement- 6: How many balls were faced by Bb McCullum in the match
bbMcCullumFilter = FILTER ipl BY batsmen1 == 'BB McCullum';
bbMcCullumGrp = GROUP bbMcCullumFilter BY 'BB McCullum';
bbMcCullumCount = FOREACH bbMcCullumGrp GENERATE COUNT(bbMcCullumFilter.batsmen1);
DUMP bbMcCullumCount;

-- Problem Statement- 7: Find the players who were run out in the match
runOutFilter = FILTER ipl BY result == 'run out';
DUMP runOutFilter;

-- Problem Statement- 8: Store the players who were run out in the match in HDFS
STORE runOutFilter INTO '/home/training/IPL/results/' USING PigStorage(',');