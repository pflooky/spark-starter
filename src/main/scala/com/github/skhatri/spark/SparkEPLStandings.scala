package com.github.skhatri.spark

import org.apache.spark.sql.DataFrame

case class HomeAwayScore(home: Int, away: Int, home_points: Int, away_points: Int) extends Serializable {
}

object SparkEPLStandings extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL to View EPL results")
    .config("spark.driver.host", "localhost")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  //Round,Date,Team 1,FT,Team 2
  val files = Seq("epl2012-part1.csv", "epl2012-part2.csv", "epl2012-part3.csv", "epl2012-part4.csv")
    .map(fileName => s"src/main/resources/$fileName")

  spark.udf.register("home_away_score", (score: String) => {
    val parts = score.split("-")
    val home = parts(0).toInt
    val away = parts(1).toInt
    val (home_points, away_points) = if (home > away) (3, 0) else if (home == away) (1, 1) else (0, 3)
    HomeAwayScore(parts(0).toInt, parts(1).toInt, home_points, away_points)
  })


  def loadData(): DataFrame = {
    val df: DataFrame = spark.read.option("header", true)
      .csv(files: _*)
      .selectExpr("week", "match_date", "home", "score", "away")
      .selectExpr("*", "home_away_score(score) as match_score")

    df.show(5, false)
    df
  }

  /**
   * This method should find and print all unique team names that participated in the Premier League season.
   * The output should be a list of team names printed to the console.
   * @param df The DataFrame containing match data.
   */
  def findTeamsInPremierLeague(df: DataFrame): Unit = {

  }

  /**
   * This method should find and print all matches (both home and away) for a specific team.
   * The output should be a list of matches for the given team, printed to the console.
   * @param df The DataFrame containing match data.
   * @param name The name of the team to find matches for.
   */
  def findAllMatchesByTeam(df: DataFrame, name: String): Unit = {

  }

  /**
   * This method should find and print the match with the biggest win (largest goal difference) by any team.
   * The output should be the details of the match with the largest goal difference, printed to the console.
   * @param df The DataFrame containing match data.
   */
  def findBiggestWinByAnyTeam(df: DataFrame): Unit = {
  }

  /**
   * This method should calculate and return the total points accumulated by a specific team.
   * Home win = 3 points, home draw = 1 point, home loss = 0 points.
   * Away win = 3 points, away draw = 1 point, away loss = 0 points.
   * @param df The DataFrame containing match data.
   * @param name The name of the team.
   * @return The total points for the specified team.
   */
  def findTeamsPoints(df: DataFrame, name: String): Int = {
    0
  }

  val df: DataFrame = loadData()
  findTeamsInPremierLeague(df)
  findAllMatchesByTeam(df, "Manchester United FC")
  findTeamsPoints(df, "Arsenal FC")
  findBiggestWinByAnyTeam(df)


}
