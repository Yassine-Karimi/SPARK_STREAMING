# Practical Activity: Spark Structured SQL

## I. Streaming Data Processing
We want to develop a Spark application for the hospital that receives incidents from the hospital in streaming using Structured Streaming. 
Incidents are streamed in CSV files (see the attached file). The data format in the CSV files is as follows:
![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/aec89716-5ae6-4414-8b7b-f9fe765eab64)

## Tasks:

* 1) Continuously display the number of incidents per service.
* 2) Continuously display the two years with the highest number of incidents.

* * The Achitect of the project: 
![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/343b9fb3-307e-4720-a9f1-b1b85a2052b6)

* 1) code
*  * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/edd05be7-cea6-46d3-a852-f5ed26e238a5)
   * By putting the incidents.csv file at the specified hadoop directory , the application will detecte the the file and compute the the number of incidents per service as follows:
   * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/ff25895f-19ad-4c94-8aa7-57e851c0f589)
   * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/f8bc0269-43df-4af1-a42b-16c29dabe279)
   * then by adding more files that contains more data about incidents the application will compute the result again on streaming ; as an exemple let's add the same file in a diffrent name as incidents1.csv:
   * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/44f1cfdc-9f38-4aaf-b70b-5cbdeff10088)
   * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/4b42ab48-a635-486e-bdfb-55ed247fa1be)


* 2) code
* * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/38bf21c8-9871-4843-a11e-70e750b243ba)
  * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/23eb0f96-325d-4f8b-993c-a94c766feb96)
  * ![image](https://github.com/Yassine-Karimi/SPARK_STREAMING/assets/66490404/b0ecbdf9-7c78-430b-ad39-35477b26dd94)





     
