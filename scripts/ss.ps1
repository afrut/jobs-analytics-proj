Clear-Host
$packages = @(
    "io.delta:delta-core_2.12:0.7.0",                   # to use Delta Lake
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3"  # to use Kafka as streaming source/sink
) -join ","

$jars = @(
    "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar"  # to connect to SQL Server
) -join ","

if($args.Length -gt 0)
{
    spark-submit --master local `
        --packages $packages `
        --jars $jars `
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" `
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" `
        --conf "spark.sql.shuffle.partitions=5" `
        @args # splat the $args array
        # $args[0] `
        # $($Env:SPARK_HOME + "\README.md")
}