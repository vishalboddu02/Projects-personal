# Databricks notebook source
# DBTITLE 1,Checking the uploaded files in the Volume
# MAGIC %fs ls dbfs:/Volumes/workspace/tata/ipl_files/

# COMMAND ----------

# DBTITLE 1,Matches -  Dataframe &  Table creation
matches_df = spark.read.option("header",True).format("csv").load("dbfs:/Volumes/workspace/tata/ipl_files/IPL_matches.csv")
#matches.write.mode("overwrite").saveAsTable("tata.matches")

# COMMAND ----------

# DBTITLE 1,Deliveries-  Dataframe &  Table creation
deliveries_df  = spark.read.option("header",True).format("csv").load("dbfs:/Volumes/workspace/tata/ipl_files/IPL_deliveries.csv")
#deliveries.write.mode("overwrite").saveAsTable("tata.deliveries")

# COMMAND ----------

# MAGIC %md 
# MAGIC #Story of Matches table:
# MAGIC This table consists data of Matches held in IPL cricket league, the includes the matches that are happened in which city,date and which teams have been participated and how many seasons played and also tells us the results of each match and umpires who umpired for respective match along with the player of the match. and we can expect insights like win percentages of a team  and its seasons performances etc.
# MAGIC
# MAGIC columns: 
# MAGIC
# MAGIC **id, season, city, date, team1, team2, toss_winner, toss_decision, result, dl_applied, winner, win_by_runs, win_by_wickets, player_of_match, venue, umpire1, umpire2, umpire3**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Story of Deliveries Table:
# MAGIC
# MAGIC This table provides the details of every ball to ball deliveries and its outcomes of the each match. it provides details of the batsmen and bowler played for every ball and its possible result. this table provides the insights of how many balls a batsmen played or a bolwer bowled and overall average of the bolwer/batsmen in the IPL stats and can get the possible total scores as per the player's performance.
# MAGIC
# MAGIC columns : 
# MAGIC
# MAGIC **match_id, inning, batting_team, bowling_team, over, ball, batsman, non_striker, bowler, is_super_over, wide_runs, bye_runs, legbye_runs, noball_runs, penalty_runs, batsman_runs, extra_runs, total_runs, player_dismissed, dismissal_kind, fielder**
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #Import statments

# COMMAND ----------


from pyspark.sql.functions import count,col,sum

# COMMAND ----------

# MAGIC %md 
# MAGIC #Question 1:
# MAGIC **Total Matches Played Each Season ~ Find the number of matches played in each IPL season.**

# COMMAND ----------

# DBTITLE 1,SOLUTION - SQL
# MAGIC %sql
# MAGIC SELECT SEASON, COUNT(*) AS MATCHES_PLAYED_PER_SEASON
# MAGIC FROM TATA.MATCHES 
# MAGIC GROUP BY SEASON
# MAGIC ORDER BY SEASON ASC

# COMMAND ----------

# DBTITLE 1,SOLUTION - PYSPARK

matches_per_season_df = (
    matches_df
              .groupBy(col("season"))
              .agg(count(col("id")).alias("matches_played_per_season"))
              .select("season", "matches_played_per_season")
              .orderBy(col("season"))

)
display(matches_per_season_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Question 2:
# MAGIC
# MAGIC **Top 5 Batsmen by Total Runs ~ Identify the top 5 batsmen who have scored the most runs across all seasons.**
# MAGIC

# COMMAND ----------

# DBTITLE 1,SOLUTION - SQL
# MAGIC %sql
# MAGIC SELECT batsman, SUM(batsman_runs) AS Runs_Scored
# MAGIC FROM TATA.DELIVERIES 
# MAGIC GROUP BY batsman
# MAGIC ORDER BY SUM(batsman_runs) DESC
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,SOLUTION - PYSPARK
top_5_batsman = (
    deliveries_df
                .groupBy("batsman")
                .agg(sum(col("batsman_runs")).alias("runs_scored"))
                .select("batsman", "runs_scored")
                .orderBy(col("runs_scored").desc())
                .limit(5)

)

display(top_5_batsman)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Question 3
# MAGIC
# MAGIC **Most Matches Played by a TeaM ~ Which team has played the most matches in IPL history?**
# MAGIC

# COMMAND ----------

# DBTITLE 1,SOLUTION - SQL
# MAGIC %sql
# MAGIC
# MAGIC SELECT TEAM, COUNT(team)  AS PLAYED_MATCHES
# MAGIC FROM 
# MAGIC (SELECT TEAM1 AS TEAM
# MAGIC FROM TATA.matches
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT TEAM2 AS TEAM
# MAGIC FROM TATA.matches) AS TEAMS
# MAGIC GROUP BY team
# MAGIC ORDER BY played_matches DESC

# COMMAND ----------

# DBTITLE 1,SOLUTION - PYSPARK
df1 = matches_df.select(col("team1").alias("Team")).unionAll(matches_df.select(col("team2").alias("Team")))

played_matches_df = (
    df1
        .groupBy(col("Team"))
        .agg(count(col("Team")).alias("Matches_played"))
        .select("Team","Matches_played")
        .orderBy(col("Matches_played").desc())
)
display(played_matches_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Question 4
# MAGIC **Total Runs Scored in Each Match ~ Calculate the total runs scored in each match using the deliveries table.**

# COMMAND ----------

# DBTITLE 1,SOLUTION - SQL
# MAGIC %sql
# MAGIC SELECT MATCH_ID, SUM(total_runs) AS RUNS_PER_MATCH
# MAGIC FROM TATA.DELIVERIES 
# MAGIC GROUP BY match_id
# MAGIC ORDER BY runs_per_match DESC

# COMMAND ----------

# DBTITLE 1,SOLUTION - PYSPARK
total_runs_per_match = (
    deliveries_df
                .groupby(col("match_id"))
                .agg(sum(col("total_runs")).alias("RUNS_PER_MATCH"))
                .select("match_id","RUNS_PER_MATCH")
                .orderBy(col("RUNS_PER_MATCH").desc())

)
display(total_runs_per_match)

# COMMAND ----------

# MAGIC %md
# MAGIC #Question 5
# MAGIC **Dismissal Types Count List each type of dismissal and how many times it occurred.**
# MAGIC

# COMMAND ----------

# DBTITLE 1,SOLUTION - SQL
# MAGIC %sql
# MAGIC SELECT dismissal_kind, count(dismissal_kind) as dismissal_counts
# MAGIC FROM TATA.DELIVERIES
# MAGIC WHERE dismissal_kind is not null
# MAGIC GROUP BY dismissal_kind
# MAGIC ORDER BY dismissal_counts DESC

# COMMAND ----------

# DBTITLE 1,SOLUTION - PYSPARK
df2 = (
    deliveries_df.where(col("dismissal_kind").isNotNull())
                .groupBy(col("dismissal_kind"))
                .agg(count(col("dismissal_kind")).alias("counts_of_dismissals"))
                .select(col("dismissal_kind"),col("counts_of_dismissals"))
                .orderBy(col("counts_of_dismissals").desc())
)
display(df2)

# COMMAND ----------

