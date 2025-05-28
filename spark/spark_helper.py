from pyspark.sql.functions import lpad, concat_ws, try_to_timestamp, col, substr, lit

def hhmm_to_timestamp(df, year_col, month_col, day_col, time_col):
    padded_time = lpad(col(time_col).cast("string"), 4, "0")
    hour = substr(padded_time, lit(1), lit(2))
    minute = substr(padded_time, lit(3), lit(2))

    datetime_str = concat_ws(" ",
        concat_ws("-", col(year_col), lpad(col(month_col), 2, "0"), lpad(col(day_col), 2, "0")),
        concat_ws(":", hour, minute)
    )

    return df.withColumn(f"{time_col}_ts", try_to_timestamp(datetime_str, "yyyy-MM-dd HH:mm"))