$arrPath = @(
"C:\WINDOWS\system32",
"D:\Python\Python39\Scripts\",
"D:\Python\Python39\",
"D:\kafka_2.13-3.1.0\bin\windows",
"C:\Program Files\Java\jdk1.8.0_202\bin",
"D:\Program Files\Git\cmd",
"D:\src\powershell-scripts\git\",
"D:\scripts",
".\scripts\"
".\scraping\driver\"
)
$Env:PATH = $arrPath -join ";"
$Env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0_202"
$Env:KAFKA_HOME="/mnt/d/kafka_2.13-3.1.0"