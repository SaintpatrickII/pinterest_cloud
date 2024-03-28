# Databricks notebook source
pin_clean = spark.read.parquet("dbfs:/user/hive/warehouse/pin_clean")
geo_clean = spark.read.parquet("dbfs:/user/hive/warehouse/geo_clean")
user_clean = spark.read.parquet("dbfs:/user/hive/warehouse/user_clean")

pin_clean.createOrReplaceTempView("pin")
geo_clean.createOrReplaceTempView("geo")
user_clean.createOrReplaceTempView("user")


# COMMAND ----------

most_popular_by_country = """
                        SELECT 
                        p.category,
                        g.country,
                        COUNT(p.category) as most_pop_cat
                        FROM pin p 
                        JOIN geo g
                        on p.ind == g.ind
                        GROUP BY g.country, p.category
                        ORDER BY most_pop_cat DESC
                        LIMIT 10;
                        """

display(spark.sql(most_popular_by_country))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC                         p.category,
# MAGIC                         g.country,
# MAGIC                         COUNT(p.category) as most_pop_cat
# MAGIC                         FROM pin_clean p 
# MAGIC                         JOIN geo_clean g
# MAGIC                         on p.ind == g.ind
# MAGIC                         GROUP BY g.country, p.category
# MAGIC                         ORDER BY most_pop_cat DESC
# MAGIC                         LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find how many posts each category had between 2018 and 2022
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   p.category,
# MAGIC   year(g.timestamp) as post_year,
# MAGIC   COUNT(p.category) as yearly_post
# MAGIC   FROM pin_clean p  
# MAGIC   JOIN geo_clean g
# MAGIC   on p.ind == g.ind
# MAGIC   -- WHERE post_year BETWEEN 2018 AND 2022
# MAGIC   GROUP BY p.category, post_year
# MAGIC   HAVING post_year BETWEEN 2018 AND 2023
# MAGIC   ORDER BY yearly_post DESC, post_year DESC
# MAGIC   
# MAGIC                         

# COMMAND ----------

# MAGIC %md
# MAGIC # Country with user with most followers:
# MAGIC
# MAGIC ## query 1 - Top post per country
# MAGIC - country
# MAGIC - poster_name
# MAGIC - follower_count
# MAGIC
# MAGIC ## query 2 - Top post worldwide
# MAGIC - country
# MAGIC - follower_count

# COMMAND ----------

# MAGIC %sql
# MAGIC -- pin.poster_name, pin.follower_count, geo.country -> join on pin.ind == geo.ind
# MAGIC
# MAGIC -- Query 1
# MAGIC WITH max_followers_per_country AS (SELECT 
# MAGIC   pin.poster_name,
# MAGIC   MAX(pin.follower_count) as total_followers,
# MAGIC   geo.country
# MAGIC FROM
# MAGIC   pin_clean pin
# MAGIC JOIN 
# MAGIC   geo_clean geo
# MAGIC ON
# MAGIC   pin.ind == geo.ind
# MAGIC GROUP BY 
# MAGIC   pin.poster_name,
# MAGIC   geo.country
# MAGIC ORDER BY total_followers DESC)
# MAGIC
# MAGIC SELECT country, total_followers
# MAGIC FROM max_followers_per_country
# MAGIC ORDER BY total_followers DESC
# MAGIC LIMIT 1
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Most popular categroy per age group:
# MAGIC ## What is the most popular category people post to based on the following age groups:
# MAGIC - 18-24
# MAGIC - 25-35
# MAGIC - 36-50
# MAGIC - +50
# MAGIC ## Your query should return a DataFrame that contains the following columns:
# MAGIC - age_group, a new column based on the original age column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output
# MAGIC
# MAGIC Required data: user.age, pin.category -> JOIN ON user.ind == pin.ind, Use case statement for new Age_group column

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WITH age_agg AS (SELECT
# MAGIC --   CASE
# MAGIC --     WHEN u.age BETWEEN 18 and 24 THEN "18-24"
# MAGIC --     WHEN u.age BETWEEN 25 and 35 THEN "25-35"
# MAGIC --     WHEN u.age BETWEEN 36 and 50 THEN "36-50"
# MAGIC --     WHEN u.age > 50 THEN "50+"
# MAGIC --     END as age_group,
# MAGIC --     p.category
# MAGIC --   FROM user_clean u
# MAGIC --   JOIN pin_clean p
# MAGIC --   ON u.ind==p.ind),
# MAGIC
# MAGIC -- SELECT 
# MAGIC --   MAX(COUNT(a.category)) AS most_pop_cat,
# MAGIC --   a.age_group
# MAGIC -- FROM 
# MAGIC --   age_agg a
# MAGIC -- GROUP BY
# MAGIC --   a.age_group DESC
# MAGIC
# MAGIC WITH age_agg AS (SELECT
# MAGIC   CASE
# MAGIC     WHEN u.age BETWEEN 18 and 24 THEN "18-24"
# MAGIC     WHEN u.age BETWEEN 25 and 35 THEN "25-35"
# MAGIC     WHEN u.age BETWEEN 36 and 50 THEN "36-50"
# MAGIC     WHEN u.age > 50 THEN "50+"
# MAGIC     END as age_group,
# MAGIC     p.category
# MAGIC   FROM user_clean u
# MAGIC   JOIN pin_clean p
# MAGIC   ON u.ind==p.ind),
# MAGIC
# MAGIC most_pop_cat AS (SELECT COUNT(category) cat_count, age_group, category FROM age_agg GROUP BY age_group, category),
# MAGIC
# MAGIC -- SELECT 
# MAGIC --   MAX(mpc.cat_count) AS category_count,
# MAGIC --   mpc.age_group,
# MAGIC --   mpc.category
# MAGIC -- FROM 
# MAGIC --   most_pop_cat mpc
# MAGIC -- GROUP BY
# MAGIC --   mpc.age_group,
# MAGIC --   mpc.category
# MAGIC -- ORDER BY category_count DESC
# MAGIC
# MAGIC ranking AS (select 
# MAGIC   age_group,
# MAGIC   category,
# MAGIC   cat_count,
# MAGIC   dense_rank() over (partition by age_group order by cat_count DESC) as rank
# MAGIC from most_pop_cat)
# MAGIC
# MAGIC SELECT * FROM ranking
# MAGIC WHERE rank = 1
# MAGIC
# MAGIC
# MAGIC -- SELECT COUNT(category), age_group, category FROM age_agg GROUP BY age_group, category
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # What is the median follower count for users in the following age groups:
# MAGIC - 18-24
# MAGIC - 25-35
# MAGIC - 36-50
# MAGIC - +50
# MAGIC ## Your query should return a DataFrame that contains the following columns:
# MAGIC - age_group, a new column based on the original age column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH user_age_followers AS (
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN u.age BETWEEN 18 and 24 THEN "18-24"
# MAGIC     WHEN u.age BETWEEN 25 and 35 THEN "25-35"
# MAGIC     WHEN u.age BETWEEN 36 and 50 THEN "36-50"
# MAGIC     WHEN u.age > 50 THEN "50+"
# MAGIC     END as age_group,
# MAGIC     p.follower_count
# MAGIC   FROM user_clean u
# MAGIC   JOIN pin_clean p
# MAGIC   ON u.ind==p.ind)
# MAGIC
# MAGIC SELECT 
# MAGIC   age_group,
# MAGIC   percentile_approx(follower_count, 0.5) AS median
# MAGIC   FROM user_age_followers
# MAGIC   GROUP BY age_group
# MAGIC   ORDER BY age_group;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Find how many users have joined between 2015 and 2020.
# MAGIC
# MAGIC ## Your query should return a DataFrame that contains the following columns:
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - number_users_joined, a new column containing the desired query output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     YEAR(date_joined) AS post_year,
# MAGIC     COUNT(*) AS number_users_joined
# MAGIC FROM user_clean
# MAGIC WHERE 
# MAGIC     YEAR(date_joined) BETWEEN 2015 AND 2020
# MAGIC GROUP BY
# MAGIC     YEAR(date_joined)
# MAGIC ORDER BY
# MAGIC     post_year DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Find the median follower count of users have joined between 2015 and 2020.
# MAGIC
# MAGIC ## Your query should return a DataFrame that contains the following columns:
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     YEAR(users.date_joined) AS post_year,
# MAGIC     percentile_approx(pin.follower_count, 0.5) AS median_follower_count
# MAGIC FROM user_clean AS users
# MAGIC JOIN pin_clean AS pin
# MAGIC     ON users.ind = pin.ind
# MAGIC WHERE
# MAGIC     YEAR(users.date_joined) BETWEEN 2015 AND 2020
# MAGIC GROUP BY
# MAGIC     YEAR(users.date_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC # Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# MAGIC
# MAGIC ## Your query should return a DataFrame that contains the following columns:
# MAGIC - age_group, a new column based on the original age column
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - median_follower_count, a new column containing the desired query output
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC       WHEN users.age >= 18 AND users.age <= 24 THEN '18-24'
# MAGIC       WHEN users.age >= 25 AND users.age <= 35 THEN '25-35'
# MAGIC       WHEN users.age >= 36 AND users.age <= 50 THEN '36-50'
# MAGIC       WHEN users.age > 50 THEN '50+'
# MAGIC   END AS age_group,
# MAGIC   YEAR(users.date_joined) AS post_year,
# MAGIC   percentile_approx(pin.follower_count, 0.5) AS median_follower_count
# MAGIC FROM user_clean AS users
# MAGIC JOIN pin_clean AS pin 
# MAGIC   ON users.ind = pin.ind
# MAGIC WHERE
# MAGIC   YEAR(users.date_joined) BETWEEN 2015 AND 2020
# MAGIC GROUP BY
# MAGIC   age_group, post_year
# MAGIC ORDER BY
# MAGIC   age_group, post_year DESC;
