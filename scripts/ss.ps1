Clear-Host
if($args.Length -gt 0)
{
    spark-submit --master local `
        --packages io.delta:delta-core_2.12:0.7.0 `
        --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar" `
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" `
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" `
        @args # splat the $args array
        # $args[0] `
        # $($Env:SPARK_HOME + "\README.md")
}