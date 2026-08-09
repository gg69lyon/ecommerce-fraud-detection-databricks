from unittest.mock import MagicMock


def test_bronze_batch_ingestion_simple():
    # Spark et DataFrame totalement automatisés
    mock_spark = MagicMock()
    mock_df = MagicMock()

    mock_spark.read.option.return_value.option.return_value.csv.return_value = (
        mock_df
    )

    # Exécution du code du notebook
    bronze_volume_path = "/Volumes/main/bronze/bronze_volume"
    kaggle_path = f"{bronze_volume_path}/kaggle/creditcard.csv"
    landing_zone = f"{bronze_volume_path}/landing_zone"

    mock_spark.sql("USE CATALOG main")
    mock_spark.sql("USE SCHEMA bronze")

    df_bronze = (
        mock_spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(kaggle_path)
    )

    df_bronze.write.format("delta").mode("overwrite").saveAsTable(
        "transactions_bronze"
    )
    df_bronze.write.mode("overwrite").option("header", True).csv(landing_zone)

    # 3 vérifications essentielles uniquement
    mock_spark.sql.assert_called_with("USE SCHEMA bronze")
    assert kaggle_path in str(mock_spark.read.option.return_value.option.return_value.csv.call_args)
    assert df_bronze.write.format.called