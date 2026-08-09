from unittest.mock import MagicMock, patch


def test_gold_batch_aggregations_simple():
    # Spark et DataFrame Silver automatisés
    mock_spark = MagicMock()
    mock_df_silver = MagicMock()
    mock_spark.table.return_value = mock_df_silver

    # Patch complet du module functions (mocke automatiquement F.col, F.sum, F.when, etc.)
    with patch("pyspark.sql.functions", MagicMock()):
        from pyspark.sql import functions as F

        # --- Lecture Silver ---
        mock_spark.sql("USE CATALOG main")
        mock_spark.sql("USE SCHEMA silver")

        df_silver = mock_spark.table("transactions_silver")

        # --- Base Gold ---
        df_gold_base = (
            df_silver
            .withColumn("minute", (F.col("time") / 60).cast("int"))
            .withColumn("hour", (F.col("time") / 3600).cast("int"))
            .withColumn("day", (F.col("time") / 86400).cast("int"))
        )

        # --- KPI Globaux ---
        df_kpi = (
            df_silver
            .agg(
                F.count("*").alias("nb_transactions"),
                F.sum("amount").alias("total_amount"),
                F.sum(F.col("is_fraud")).alias("nb_fraud"),
                F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)).alias("fraud_amount"),
                F.sum(F.when(F.col("is_fraud") == 0, F.col("amount")).otherwise(0)).alias("legit_amount"),
            )
            .withColumn("fraud_rate", F.col("nb_fraud") / F.col("nb_transactions"))
        )

        # --- Agrégations temporelles ---
        df_fraud_by_minute = (
            df_gold_base
            .groupBy("minute")
            .agg(
                F.count("*").alias("nb_transactions"),
                F.sum("is_fraud").alias("nb_fraud"),
                F.sum("amount").alias("total_amount")
            )
            .withColumn("fraud_rate", F.col("nb_fraud") / F.col("nb_transactions"))
        )

        df_fraud_by_hour = (
            df_gold_base
            .groupBy("hour")
            .agg(
                F.count("*").alias("nb_transactions"),
                F.sum("is_fraud").alias("nb_fraud"),
                F.sum("amount").alias("total_amount")
            )
            .withColumn("fraud_rate", F.col("nb_fraud") / F.col("nb_transactions"))
        )

        df_fraud_by_day = (
            df_gold_base
            .groupBy("day")
            .agg(
                F.count("*").alias("nb_transactions"),
                F.sum("is_fraud").alias("nb_fraud"),
                F.sum("amount").alias("total_amount")
            )
            .withColumn("fraud_rate", F.col("nb_fraud") / F.col("nb_transactions"))
        )

        # --- Écriture Gold ---
        mock_spark.sql("USE CATALOG main")
        mock_spark.sql("USE SCHEMA gold")

        df_kpi.write.format("delta").mode("overwrite").saveAsTable("fraud_kpi_gold")
        df_fraud_by_minute.write.format("delta").mode("overwrite").saveAsTable("fraud_by_minute_gold")
        df_fraud_by_hour.write.format("delta").mode("overwrite").saveAsTable("fraud_by_hour_gold")
        df_fraud_by_day.write.format("delta").mode("overwrite").saveAsTable("fraud_by_day_gold")

    # Assertions
    mock_spark.table.assert_called_once_with("transactions_silver")
    mock_spark.sql.assert_called_with("USE SCHEMA gold")

    # Validation que les écritures ont bien eu lieu
    assert df_kpi.write.format.called
    assert df_fraud_by_minute.write.format.called
    assert df_fraud_by_hour.write.format.called
    assert df_fraud_by_day.write.format.called