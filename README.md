# Spark Streaming

## How to Run the Application

1. Go to `Run` tab and click on `Edit Configurations...`
2. Click at the `+` sign and choose `Application`
3. Choose your `Module` and `Main Class`
4. Open the `VM Options` and copy / paste (and alter if you want) the following:
    ```
    -Dlog4j.configuration=file:log4j.properties
    -Dlogfile.name=application
    -Dspark.yarn.app.container.log.dir=app-logs/example4
    ```
    * **Dlog4j.configuration** corresponds to the `log4j.properties` file at the root directory.
    * **Dlogfile.name** will be the name of your log file.
    * **Dspark.yarn.app.container.log.dir** corresponds to the directory in which the log file will be saved.


5. Your `Program Arguments` will be the following: **data/sample.csv** 
    * That's the location of the file processed in the examples
6. Run the Application

