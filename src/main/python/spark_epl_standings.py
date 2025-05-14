from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, IntegerType

# Define a schema for the UDF's return type, similar to the Scala case class
home_away_score_schema = StructType([
    StructField("home_goals", IntegerType(), True),
    StructField("away_goals", IntegerType(), True),
    StructField("home_points", IntegerType(), True),
    StructField("away_points", IntegerType(), True)
])

def calculate_home_away_score(score: str):
    parts = score.split("-")
    home = int(parts[0])
    away = int(parts[1])
    if home > away:
        home_points, away_points = 3, 0
    elif home == away:
        home_points, away_points = 1, 1
    else:
        home_points, away_points = 0, 3
    return home, away, home_points, away_points

def main():
    spark = SparkSession \
        .builder \
        .appName("PySpark SQL to View EPL results") \
        .config("spark.driver.host", "localhost") \
        .master("local[2]") \
        .getOrCreate()

    # Register the UDF
    spark.udf.register("home_away_score_udf", calculate_home_away_score, home_away_score_schema)

    files = [f"src/main/resources/{file_name}" for file_name in ["epl2012-part1.csv", "epl2012-part2.csv", "epl2012-part3.csv", "epl2012-part4.csv"]]

    def load_data() -> DataFrame:
        df = spark.read.option("header", True) \
            .csv(files) \
            .withColumnRenamed("Team 1", "home") \
            .withColumnRenamed("FT", "score") \
            .withColumnRenamed("Team 2", "away") \
            .withColumnRenamed("Round", "week") \
            .withColumnRenamed("Date", "match_date") \
            .select("week", "match_date", "home", "score", "away") \
            .withColumn("match_score", udf(calculate_home_away_score, home_away_score_schema)(col("score")))

        df.show(5, False)
        return df

    '''
    This method should find and print all unique team names that participated in the Premier League season.
    The output should be a list of team names printed to the console.
    :param df: The DataFrame containing match data.
    '''
    def find_teams_in_premier_league(df: DataFrame):
        pass

    '''
    This method should find and print all matches (both home and away) for a specific team.
    The output should be a list of matches for the given team, printed to the console.
    :param df: The DataFrame containing match data.
    :param name: The name of the team to find matches for.
    '''
    def find_all_matches_by_team(df: DataFrame, name: str):
        pass

    '''
    This method should find and print the match with the biggest win (largest goal difference) by any team.
    The output should be the details of the match with the largest goal difference, printed to the console.
    :param df: The DataFrame containing match data.
    '''
    def find_biggest_win_by_any_team(df: DataFrame):
        pass

    '''
    This method should calculate and return the total points accumulated by a specific team.
    Home win = 3 points, home draw = 1 point, home loss = 0 points.
    Away win = 3 points, away draw = 1 point, away loss = 0 points.
    :param df: The DataFrame containing match data.
    :param name: The name of the team.
    :return: The total points for the specified team.
    '''
    def find_teams_points(df: DataFrame, name: str) -> int:
        return 0

    df_data = load_data()
    find_teams_in_premier_league(df_data)
    find_all_matches_by_team(df_data, "Manchester United FC")
    find_teams_points(df_data, "Arsenal FC")
    find_biggest_win_by_any_team(df_data)

    spark.stop()

if __name__ == "__main__":
    main() 