# Databricks notebook source
server_name = "jdbc:sqlserver://xxxxxxxxxxxxxx.database.windows.net"
database_name = "xxxxxxxxxxxxxxxxxxx"
table_name = "xxxxxxxxxxxxxxxx"
username = "xxxxxxxxxxxxxx"
password = "xxxxxxxxxxxxxxxxxxxxxxxxxx" # Please specify password here
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"



jdbcUrl = server_name + ";" + "databaseName=" + database_name + ";"




# Create a data frame by reading data from SQL Server via JDBC
#jdbcDF = spark.read.format("jdbc") \
# .option("url", jdbcUrl) \
# .option("dbtable", "dbo.tran_data_codes") \
# .option("user", username) \
# .option("password", password) \
# .option("driver", jdbcDriver) \
# .load()
#jdbcDF.show()



# We can also save the data frame to the database via JDBC too.
jdbcDF = spark.table('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx').write.format("jdbc") \
.mode("overwrite") \
.option("url", jdbcUrl) \
.option("dbtable", "dbo.tran_data_codes") \
.option("user", username) \
.option("password", password) \
.save()

# COMMAND ----------


