from unittest.mock import MagicMock, patch


def test_silver_batch_transformation_simple():
    # Spark et DataFrame Bronze automatisés
    mock_spark = MagicMock()
    mock_df_bronze = MagicMock()
    mock_spark.table.return_value = mock_df_bronze

    # On mock F.col pour éviter qu'il cherche Java / SparkContext
    with patch("pyspark.sql.functions.col", return_value=MagicMock()):

        # Code du notebook exécuté
        mock_spark.sql("USE CATALOG main")
        mock_spark.sql("USE SCHEMA bronze")

        df_bronze = mock_spark.table("transactions_bronze")

        # Import local de F pour utiliser le mock
        from pyspark.sql import functions as F

        df_silver = (
            df_bronze.na.drop(subset=["Amount", "Class"])
            .withColumn("Amount", F.col("amount").cast("double"))
            .withColumn("is_fraud", F.col("Class").cast("int"))
            .dropDuplicates()
            .drop("Class")
            .withColumnRenamed("Amount", "amount")
            .withColumnRenamed("Time", "time")
        )

        mock_spark.sql("USE CATALOG main")
        mock_spark.sql("USE SCHEMA silver")

        df_silver.write.format("delta").mode("overwrite").saveAsTable(
            "transactions_silver"
        )

    # Vérifications clés
    mock_spark.table.assert_called_once_with("transactions_bronze")
    mock_df_bronze.na.drop.assert_called_once_with(subset=["Amount", "Class"])
    mock_spark.sql.assert_called_with("USE SCHEMA silver")
    assert df_silver.write.format.called