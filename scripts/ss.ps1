Clear-Host
if($args.Length -gt 0)
{
    spark-submit --master local `
        --packages io.delta:delta-core_2.12:0.7.0 `
        --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar" `
        $args[0]
        # $args[0] `
        # $($Env:SPARK_HOME + "\README.md")
}