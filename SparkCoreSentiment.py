from pyspark.sql import SparkSession
import csv 
import re
from tabulate import tabulate
import datetime
import sys
from pympler import asizeof

# regex patter for filtering non numbers 
pattern = re.compile(r"([0-9]*[.])?[0-9]+")

spark = (SparkSession.builder
        .appName("textAnalysis")
        .master("local[*]")
        .config("spark.executor.heartbeatInterval", "300s")
        .config("spark.network.timeout", "600s")  
        .config("spark.driver.memory", "8g") 
        .config("spark.executor.memory", "8g") .config("spark.driver.maxResultSize", "8g") 
        .getOrCreate())

spark.conf.set("spark.sql.repl.eagerEval.enabled", True) #  This will format our output tables a bit nicer when not using the show() method
sc = spark.sparkContext

data_to_analyze = sc.textFile("steam_reviews.csv")


# -- cleaning data -- 
intial_time = datetime.datetime.now()
print("Before all " , intial_time)

# removing the header 
header = data_to_analyze.first()
rdd = data_to_analyze.filter(lambda row: row != header)

# adding columns by splitting on the comma
rdd = rdd.map(lambda row: row.split(','))
rdd = rdd.filter(lambda row: len(row) > 21)

# filtering out where ids arent ids and names arent names
rdd = rdd.filter(lambda row: row[2][0:].isdigit() == False)
rdd = rdd.filter(lambda row: row[1][0:].isdigit() == True)
# sometimes the game id is actually a review id
rdd = rdd.filter(lambda row: (int(row[1]) > 235) )

# making sure the playtime is a number
rdd = rdd.filter(lambda row: pattern.fullmatch(row[-2]) is not None)

# Grouping by app name and id if recommended = true 
grouped_by_recommended = rdd.map(lambda row: ((row[1], row[2]), (1, 1 if row[8].strip().lower() == 'true' else 0)))

recommended_reduced = grouped_by_recommended.reduceByKey(lambda x, y: (x[0] + x[1], y[0] + y[1]))

def calculate_sentiment(row):
    (game_id, game_name), (total_reviews, recommended_count) = row
    recommended_percentage = (recommended_count / total_reviews) * 100

    # Categorize based on the recommended percentage
    if recommended_percentage >= 95:
        result = "Overwhelmingly Positive"
    elif recommended_percentage >= 80:
        result = "Very Positive"
    elif recommended_percentage >= 70:
        result = "Mostly Positive"
    elif recommended_percentage >= 40:
        result = "Mixed"
    elif recommended_percentage >= 20:
        result = "Mostly Negative"
    else:
        result = "Overwhelmingly Negative"

    return ((game_id, game_name), (recommended_percentage, result))

recommended_levels = recommended_reduced.map(calculate_sentiment)

# grouping playtime by the game id and name then the mins and 1 (counter)
grouped_by_playtime = rdd.map(lambda row: ((row[1], row[2]), (float(row[-2]), 1))) 

# Aggregate total reviews and total playtime
playtime_reduced = grouped_by_playtime.reduceByKey(lambda x, y: (x[0] + y[0], x[1] +y[1]))

playtime_reduced = playtime_reduced.filter(lambda row :  row[1][0] > 0 and row[1][1] > 0)

def calculate_playtime(row):
    (game_id, game_name), ( playtime_at_review, total_reviews) = row
    playtime_hours = playtime_at_review / 60
    avg_playtime = playtime_hours / total_reviews
    return ((game_id, game_name), (avg_playtime))

avg_playtime = playtime_reduced.map(calculate_playtime)

# joining results and flattening from tuples 
joined_results = recommended_levels.join(avg_playtime).map(lambda row: (row[0][0], row[0][1], row[1][0][0], row[1][0][1], row[1][1])).sortBy(lambda row: row[-1]).collect()
print("AFTER JOIN + SORT" , datetime.datetime.now(),)

header = ["Game Id", "Game name", "Percentage Recommended", "Overall Review feeling", "Average playtime at review"]
print(tabulate(joined_results, headers=header, tablefmt="grid"))


print( " time taken = ", datetime.datetime.now() - intial_time)



# Estimate memory size in bytes
memory_usage = sys.getsizeof(joined_results) + sum(sys.getsizeof(row) for row in joined_results)

# Convert to MB
memory_usage_mb = asizeof.asizeof(joined_results) / (1000000)
print(f"Rough Memory Usage: {memory_usage_mb:.2f} MB")
print(f"Rough Memory Usage of RDD: {asizeof.asizeof(rdd.collect()):.2f} MB")


