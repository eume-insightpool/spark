from pyspark.sql import hiveCtx
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row

hiveCtx = HiveContext(sc)
#esJsons = hiveCtx.read.json('/es/ppl.bulk')
#esJsons = hiveCtx.read.json(inputFile)
esJsons = hiveCtx.read.json('/user/hdfs/datata')
esJsons.registerTempTable("esJsonsTable")
createRatios = hiveCtx.sql("""
SELECT a.twitter_id as twitter_id,
DATEDIFF(a.last_status_time,a.account_creation_date) as lasttweet_creation,
a.friends_count as nfriends,
a.friends_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date) as nfriends_rate,
DATEDIFF(a.last_status_time,a.account_creation_date) / DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.last_status_time) as update_creation_ratio,
DATEDIFF(a.updated_at,a.account_creation_date) / DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.last_status_time) as update_today_ratio,
DATEDIFF(a.updated_at,a.account_creation_date) as update_creation_ratio2,
a.followers_count as nFollowers,(a.listed_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.updated_at) as nlists_rate_adj,
DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date) as date_to_creation,
a.listed_count as nlists,
DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date) as nlist_rate,
(a.friends_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date) as nfriends_adj,
(a.followers_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.updated_at) as nfollowers_adj,
(a.followers_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/DATEDIFF(a.last_status_time,a.updated_at) as nfollowers_adj2,
(a.listed_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/DATEDIFF(a.last_status_time,a.updated_at) as nlists_rate_adj2,
a.recent_kscore/DATEDIFF(current_timestamp(),a.last_status_time) as adj_klout_score,
(a.followers_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.last_status_time)) as nfollowers_rate,
a.recent_kscore as kScore,
a.recent_kscore/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.last_status_time) as adj_kscore,
(a.friends_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.updated_at) as adj_adj_friends,
a.recent_kscore/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date) adj_kscore2,
(a.friends_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/(a.statuses_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date)) as friends_tweet_rate,
length(a.twitter_description) as description_length,
(a.followers_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/a.recent_kscore as kscore_nfollowers,
(a.friends_count/DATEDIFF(CONCAT(YEAR(current_date()),"-",MONTH(current_date()),"-",DAY(current_date())),a.account_creation_date))/a.recent_kscore as kscore_nfriends
FROM esJsonsTable AS a
""")
createRatios.registerTempTable("createRatiosTable")


calculateMean = hiveCtx.sql("""
SELECT  
AVG(lasttweet_creation) AS lasttweet_creation, AVG(nfriends) AS nfriends, AVG(nfriends_rate) as nfriends_rate, AVG(update_creation_ratio) AS update_creation_ratio, AVG(update_today_ratio) AS update_today_ratio, AVG(update_creation_ratio2) AS update_creation_ratio2, AVG(nFollowers) AS nFollowers, AVG(nlists_rate_adj) AS nlists_rate_adj, AVG(nlists) AS nlists, AVG(nlist_rate) AS nlist_rate, AVG(nfriends_adj) AS nfriends_adj, AVG(nfollowers_adj) AS nfollowers_adj, AVG(nfollowers_adj2) AS nfollowers_adj2, AVG(nlists_rate_adj2) AS nlists_rate_adj2, AVG(adj_klout_score) AS adj_klout_score, AVG(kScore) AS kScore, AVG(adj_kscore) AS adj_kscore, AVG(adj_adj_friends) AS adj_adj_friends, AVG(adj_kscore2) AS adj_kscore2, AVG(friends_tweet_rate) AS friends_tweet_rate, AVG(description_length) AS description_length, AVG(kscore_nfollowers) AS kscore_nfollowers, AVG(kscore_nfriends) AS kscore_nfriends, AVG(date_to_creation) AS date_to_creation, AVG(nfollowers_rate) AS nfollowers_rate
FROM createRatiosTable
""")
calculateMean.registerTempTable("mean")

######Standardization Process#########
calculateSqrdDev = hiveCtx.sql(""" 
SELECT 
sqrt(SUM((a.nfriends-b.nfriends)*(a.nfriends-b.nfriends))/COUNT(*)) as nfriends,
sqrt(SUM((a.lasttweet_creation-b.lasttweet_creation)*(a.lasttweet_creation-b.lasttweet_creation))/COUNT(*)) AS lasttweet_creation,
sqrt(SUM((a.nfriends_rate-b.nfriends_rate)*(a.nfriends_rate-b.nfriends_rate))/COUNT(*)) AS nfriends_rate,
sqrt(SUM((a.update_creation_ratio-b.update_creation_ratio)*(a.update_creation_ratio-b.update_creation_ratio))/COUNT(*)) AS update_creation_ratio,
sqrt(SUM((a.update_today_ratio-b.update_today_ratio)*(a.update_today_ratio-b.update_today_ratio))/COUNT(*)) AS update_today_ratio,
sqrt(SUM((a.update_creation_ratio2-b.update_creation_ratio2)*(a.update_creation_ratio2-b.update_creation_ratio2))/COUNT(*)) AS update_creation_ratio2,
sqrt(SUM((a.nFollowers-b.nFollowers)*(a.nFollowers-b.nFollowers))/COUNT(*)) AS nFollowers,
sqrt(SUM((a.nlists_rate_adj-b.nlists_rate_adj)*(a.nlists_rate_adj-b.nlists_rate_adj))/COUNT(*)) AS nlists_rate_adj,
sqrt(SUM((a.nlists-b.nlists)*(a.nlists-b.nlists))/COUNT(*)) AS nlists,
sqrt(SUM((a.nlist_rate-b.nlist_rate)*(a.nlist_rate-b.nlist_rate))/COUNT(*)) AS nlist_rate,
sqrt(SUM((a.nfriends_adj-b.nfriends_adj)*(a.nfriends_adj-b.nfriends_adj))/COUNT(*)) AS nfriends_adj,
sqrt(SUM((a.nfollowers_adj-b.nfollowers_adj)*(a.nfollowers_adj-b.nfollowers_adj))/COUNT(*)) AS nfollowers_adj,
sqrt(SUM((a.nfollowers_adj2-b.nfollowers_adj2)*(a.nfollowers_adj2-b.nfollowers_adj2))/COUNT(*)) AS nfollowers_adj2,
sqrt(SUM((a.nlists_rate_adj2-b.nlists_rate_adj2)*(a.nlists_rate_adj2-b.nlists_rate_adj2))/COUNT(*)) AS nlists_rate_adj2,
sqrt(SUM((a.adj_klout_score-b.adj_klout_score)*(a.adj_klout_score-b.adj_klout_score))/COUNT(*)) AS adj_klout_score,
sqrt(SUM((a.kScore-b.kScore)*(a.kScore-b.kScore))/COUNT(*)) AS kScore,
sqrt(SUM((a.adj_kscore-b.adj_kscore)*(a.adj_kscore-b.adj_kscore))/COUNT(*)) AS adj_kscore,
sqrt(SUM((a.adj_adj_friends-b.adj_adj_friends)*(a.adj_adj_friends-b.adj_adj_friends))/COUNT(*)) AS adj_adj_friends,
sqrt(SUM((a.adj_kscore2-b.adj_kscore2)*(a.adj_kscore2-b.adj_kscore2))/COUNT(*)) AS adj_kscore2,
sqrt(SUM((a.friends_tweet_rate-b.friends_tweet_rate)*(a.friends_tweet_rate-b.friends_tweet_rate))/COUNT(*)) AS friends_tweet_rate,
sqrt(SUM((a.description_length-b.description_length)*(a.description_length-b.description_length))/COUNT(*)) AS description_length,
sqrt(SUM((a.kscore_nfollowers-b.kscore_nfollowers)*(a.kscore_nfollowers-b.kscore_nfollowers))/COUNT(*)) AS kscore_nfollowers,
sqrt(SUM((a.kscore_nfriends-b.kscore_nfriends)*(a.kscore_nfriends-b.kscore_nfriends))/COUNT(*)) AS kscore_nfriends,
sqrt(SUM((a.date_to_creation-b.date_to_creation)*(a.date_to_creation-b.date_to_creation))/COUNT(*)) AS date_to_creation,
sqrt(SUM((a.nfollowers_rate-b.nfollowers_rate)*(a.nfollowers_rate-b.nfollowers_rate))/COUNT(*)) AS nfollowers_rate
FROM createRatiosTable AS a, mean as b
""")
calculateSqrdDev.registerTempTable("stDevTable")


standardization = hiveCtx.sql("""
SELECT 
crt.twitter_id as twitter_id,
(crt.nfollowers_rate-mean.nfollowers_rate)/(std.nfollowers_rate) AS nfollowers_rate,
(crt.nfriends-mean.nfriends)/std.nfriends as nfriends,
(crt.lasttweet_creation-mean.lasttweet_creation)/std.lasttweet_creation AS lasttweet_creation,
(crt.nfriends_rate-mean.nfriends_rate)/std.nfriends_rate AS nfriends_rate,
(crt.update_creation_ratio-mean.update_creation_ratio)/std.update_creation_ratio AS update_creation_ratio,
(crt.update_today_ratio-mean.update_today_ratio)/std.update_today_ratio AS update_today_ratio,
(crt.update_creation_ratio2-mean.update_creation_ratio2)/std.update_creation_ratio2 AS update_creation_ratio2,
(crt.nFollowers-mean.nFollowers)/std.nFollowers AS nFollowers,
(crt.nlists_rate_adj-mean.nlists_rate_adj)/std.nlists_rate_adj AS nlists_rate_adj,
(crt.nlists-mean.nlists)/std.nlists AS nlists,
(crt.nlist_rate-mean.nlist_rate)/std.nlist_rate AS nlist_rate,
(crt.nfriends_adj-mean.nfriends_adj)/std.nfriends_adj AS nfriends_adj,
(crt.nfollowers_adj-mean.nfollowers_adj)/std.nfollowers_adj AS nfollowers_adj,
(crt.nfollowers_adj2-mean.nfollowers_adj2)/std.nfollowers_adj2 AS nfollowers_adj2,
(crt.nlists_rate_adj2-mean.nlists_rate_adj2)/std.nlists_rate_adj2 AS nlists_rate_adj2,
(crt.adj_klout_score-mean.adj_klout_score)/std.adj_klout_score AS adj_klout_score,
(crt.kScore-mean.kScore)/std.kScore AS kScore,
(crt.adj_kscore-mean.adj_kscore)/std.adj_kscore AS adj_kscore,
(crt.adj_adj_friends-mean.adj_adj_friends)/std.adj_adj_friends AS adj_adj_friends,
(crt.adj_kscore2-mean.adj_kscore2)/std.adj_kscore2 AS adj_kscore2,
(crt.friends_tweet_rate-mean.friends_tweet_rate)/std.friends_tweet_rate AS friends_tweet_rate,
(crt.description_length-mean.description_length)/std.description_length AS description_length,
(crt.kscore_nfollowers-mean.kscore_nfollowers)/std.kscore_nfollowers AS kscore_nfollowers,
(crt.kscore_nfriends-mean.kscore_nfriends)/std.kscore_nfriends AS kscore_nfriends,
(crt.date_to_creation-mean.date_to_creation)/std.date_to_creation AS date_to_creation
FROM createRatiosTable as crt, stDevTable as std, mean as mean
""")

standardization.registerTempTable("standardizedTable")

addWeights = hiveCtx.sql("""
SELECT 
twitter_id AS twitter_id,
nfriends*0.10254687978388011 AS nfriendss,
lasttweet_creation*0.09035039549179855 AS lasttweet_creation,
nfriends_rate*0.061246334797442244 AS nfriends_rate,
update_creation_ratio*0.05975262763594667 AS update_creation_ratio,
update_today_ratio*0.05975262763594667 AS update_today_ratio,
update_creation_ratio2*0.05439131337309184 AS update_creation_ratio2,
nFollowers*0.05420499162937828 AS nFollowers,
nlists_rate_adj*0.05363327932655686 AS nlists_rate_adj,
date_to_creation*0.053605051297661827 AS date_to_creation,
nlists*0.04573430849273936 AS nlists,
nlist_rate*0.03838845731098478 AS nlist_rate,
nfriends_adj*0.037573971278668586 as nfriends_adj,
nfollowers_adj*0.034422174986368174 AS nfollowers_adj,
nfollowers_adj2*0.03290981974676826 AS nfollowers_adj2,
nlists_rate_adj2*0.03173896696286107 AS nlists_rate_adj2,
adj_klout_score*0.029660802104764263 AS adj_klout_score,
nfollowers_rate*0.027497768479132056 AS nfollowers_rate,
kScore*0.02548056450244698 AS kScore,
adj_kscore*0.02062067831326193 AS adj_kscore,
adj_adj_friends*0.019300396593085793 AS adj_adj_friends,
adj_kscore2*0.01908569497384983 AS adj_kscore2,
friends_tweet_rate*0.01602516107043425 AS friends_tweet_rate,
description_length*0.013401088250721338 AS description_length,
kscore_nfollowers*0.0117093028669225 AS kscore_nfollowers,
kscore_nfriends*0.011554439104648987 AS kscore_nfriends
FROM standardizedTable
""")

addWeights.registerTempTable("addWeightsTable")

all = hiveCtx.sql("""
SELECT * FROM addWeightsTable
""")

sumWeights = hiveCtx.sql("""
SELECT twitter_id AS twitter_id, (nfriendss+lasttweet_creation+nfriends_rate+update_creation_ratio+update_today_ratio+update_creation_ratio2+nFollowers+nlists_rate_adj+date_to_creation+nlists+nlist_rate+nfriends_adj+nfollowers_adj+nfollowers_adj2+nlists_rate_adj2+adj_klout_score+nfollowers_rate+kScore+adj_kscore+adj_adj_friends+adj_kscore2+friends_tweet_rate+description_length+kscore_nfollowers+kscore_nfriends) as weighedSum FROM addWeightsTable
""")
sumWeights.registerTempTable("sumWeightsTempTable")

hiveCtx.sql("""CREATE TABLE IF NOT EXISTS iprec_output(twiter_id STRING, score STRING)""")

hiveCtx.sql("""INSERT OVERWRITE TABLE iprec_output SELECT * FROM sumWeightsTempTable""")
