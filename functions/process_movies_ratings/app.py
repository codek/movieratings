"""
Read the files (Could get it too using reqests)
For now assume they are in /tmp
Then join and dump to arrow
"""
import os
import logging
import pandas as pd
import pyarrow as pa

ENVIRONMENT = os.environ["ENVIRONMENT"]
LOG_LEVEL = os.getenv("LOG_LEVEL", default="INFO")

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


def lambda_handler(event, _):
    """
    Called from init, or as the initial function in lambda
    """
    logger.info(f"Process movie ratings starting with: {event}")

    # Task Step 1. Read the files

    logger.info("Reading ratings")
    # Format: UserID::MovieID::Rating::Timestamp
    # 4th field is an epoch, so use df[Timestamp]=pd.to_datetime(df[Timestamp],unit='s')
    # In this case, lets be explicit in types, since the file has a provided definition.
    # For now; maintain the source name
    ratings_types = {0: "int", 1: "int", 2: "int", 3: "int"}
    ratings_df = pd.read_csv(
        "/tmp/ratings.dat", sep="::", engine="python", dtype=ratings_types
    )
    ratings_df.columns = ["UserID", "MovieID", "Rating", "Timestamp"]

    # check for gotchas in the exercise
    logger.info(ratings_df.agg(["min", "max", "count"]))

    logger.info("Reading movies")
    # Format: MovieID::Title::Genres
    # Note: the first gotcha of the exercise, the encoding of the movies file.
    movies_types = {0: "int", 1: "string", 2: "string"}
    movies_df = pd.read_csv(
        "/tmp/movies.dat",
        sep="::",
        engine="python",
        encoding="ISO-8859-1",
        dtype=movies_types,
    )
    movies_df.columns = ["MovieID", "Title", "Genres"]

    # check for gotchas in the exercise
    logger.info(movies_df.agg(["min", "max", "count"]))

    # Task Step 2. Creates a new dataframe, which contains
    # the movies data and 3 new columns max, min and
    # average rating for that movie from the ratings data.

    merged_df = pd.merge(ratings_df, movies_df, on="MovieID")
    movie_ratings_summary = (
        merged_df.groupby(["MovieID", "Title", "Genres"])
        .agg(
            max_rating=("Rating", "max"),
            min_rating=("Rating", "min"),
            avg_rating=("Rating", "mean"),
        )
        .reset_index()
    )
    logger.info(movie_ratings_summary)
    # Note: It looks like there are some movies that have no ratings.

    # Task Step 3. Create a new dataframe which contains
    # each user’s (userId in the ratings data) top 3 movies
    # based on their rating

    sorted_ratings = ratings_df.sort_values(
        by=["UserID", "Rating"], ascending=[True, False]
    )
    top3_movies_by_user = sorted_ratings.groupby("UserID").head(3)
    logger.info(top3_movies_by_user)

    # Task Step 4. Dump to arrow
    dump_to_arrow("top3_movies_by_user", top3_movies_by_user)
    dump_to_arrow("movie_ratings_summary", movie_ratings_summary)


def dump_to_arrow(filename, df):
    """
    Take a df, and dump to arrow using the filename provided
    """

    # Always declare in a var so we can be verbose in logs
    # TODO Take the /tmp from an env var.
    arrow_filename = f"/tmp/{filename}.arrow"
    logger.info(f"Writing to: {arrow_filename}")

    table = pa.Table.from_pandas(df)
    with pa.OSFile(arrow_filename, "wb") as arrow_file:
        with pa.RecordBatchFileWriter(arrow_file, table.schema) as writer:
            writer.write_table(table)


# test test...
if __name__ == "__main__":
    lambda_handler({}, None)
