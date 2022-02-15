import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, MapType

import re
from pyspark.sql.functions import udf
import uuid

os.environ['AWS_ACCESS_KEY_ID'] = "<ENTER AWS ACCESS KEY ID>"
os.environ['AWS_SECRET_ACCESS_KEY'] = "<ENTER AWS SECRET ACCESS KEY>"

# local path on EMR which is the HDFS
INPUT_PATH = "/raw-data" 
OUTPUT_PATH = "/clean-data"


@udf(MapType(StringType(),StringType()))
def parseCountriesUDF(line):
    """
    Parse country code and country name from string
    """
    line = line.strip()
    PATTERN = '^([0-9]+) (\s*=\s*) (\')(.+)(\')$'
    match = re.search(PATTERN, line)
    return {
        "int_country_code" : match.group(1).strip(), 
        "int_country_name" : match.group(4).strip(), 
    }

@udf(StringType())
def parseISOCodesUDF(string):
    """
    Parse iso code from string
    """
    string = string.strip()
    PATTERN = '^([A-Z]+) (\s*\/\s*) ([A-Z]+)$'
    match = re.search(PATTERN, string)
    return match.group(1)

@udf(StringType())
def parseGdpUsdUDF(string):
    """
    Parse GDP from string
    """
    if string:
        string = string.strip()
        PATTERN = '^([0-9\.]+)'
        match = re.search(PATTERN, string)
        return match.group(1)
    else:
        return None

@udf(MapType(StringType(),StringType()))
def parsePortOfEntryUDF(line):
    """
    Parse port of entry code and port of entry name from string
    """
    line = line.strip()
    PATTERN = '^(\')(.+)(\')(\s*)(=)(\s*)(\')(.+)(\')$'
    match = re.search(PATTERN, line)
    return {
        "port_of_entry_code" : match.group(2).strip(), 
        "port_of_entry_name" : match.group(8).strip(), 
    }

def convert_sas_date(x):
    """
    Converts SAS date to date
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None

def register_udfs(spark):
    """
    Register UDFs
    """
    # register UDF to convert SAS date to date
    spark.udf.register("convert_sas_date", lambda x: convert_sas_date(x), DateType())
    
    # register UDF for generating uuid
    spark.udf.register("gen_uuid", lambda : str(uuid.uuid4()))


def create_spark_session():
    """
    create a spark session with hadoop-aws package
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2") \
        .getOrCreate()
    
    """ 
    conf = SparkConf()
    conf.set("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.1.2")
    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    """ 

    return spark


def process_countries(spark):
    # read the internal_country_codes text file
    df_internal_countries = spark.read.text(INPUT_PATH+"/internal_country_codes.txt")

    # parse the internal_country_codes data
    dfParsed = df_internal_countries.withColumn("parsed", parseCountriesUDF("value"))
    fields = ["int_country_code", "int_country_name"]
    exprs = [ "parsed['{}'] as {}".format(field,field) for field in fields]
    dfClean = dfParsed.selectExpr(*exprs)
    
    # create staging_int_countries temp-view
    dfClean.createOrReplaceTempView("staging_int_countries")

    # schema for country_codes 
    country_codes_schema = StructType([
        StructField("COUNTRY",StringType()),
        StructField("COUNTRY CODE",IntegerType()),
        StructField("ISO CODES",StringType()),
        StructField("POPULATION",IntegerType()),
        StructField("AREA KM2",DoubleType()),
        StructField("GDP $USD",StringType()),
    ])

    # read country_codes data
    df_country_codes = spark.read.csv(INPUT_PATH+"/country_codes.csv", schema=country_codes_schema, header=True)

    # rename columns
    df_country_codes = df_country_codes.withColumnRenamed("COUNTRY","country_name") \
        .withColumnRenamed("COUNTRY CODE","country_code") \
        .withColumnRenamed("ISO CODES","iso_codes") \
        .withColumnRenamed("POPULATION","population") \
        .withColumnRenamed("AREA KM2","area_km2") \
        .withColumnRenamed("GDP $USD","gdp_usd")
    
    df_parsed_country_codes = df_country_codes.withColumn("country_iso_code", parseISOCodesUDF("iso_codes"))\
                                          .withColumn("gdp_usd_billion", parseGdpUsdUDF("gdp_usd"))
    
    # create staging_country_codes temp-view
    df_parsed_country_codes.createOrReplaceTempView("staging_country_codes")

    # reading unmatched countries data. This data was manually processed outside of the etl.
    df_unmatched_countries_updated = spark.read.csv(INPUT_PATH+"/unmatched_countries_updated.csv", header=True)
    df_unmatched_countries_updated.createOrReplaceTempView("unmatched_countries_updated")

    # create unmatched_countries_final dataframe
    df_unmatched_countries_final = spark.sql("""
        SELECT int_country_code, NVL(actual_country_name, INITCAP(int_country_name)) int_country_name
        FROM unmatched_countries_updated
    """)

    # create unmatched_countries temp-view
    df_unmatched_countries_final.createOrReplaceTempView("unmatched_countries")

    # create staging_int_countries_final dataframe
    df_staging_int_countries_final = spark.sql("""    
        SELECT ic.int_country_code, NVL(uc.int_country_name, ic.int_country_name) int_country_name
        FROM staging_int_countries ic
        LEFT JOIN unmatched_countries uc ON ic.int_country_code = uc.int_country_code
    """)

    df_staging_int_countries_final.createOrReplaceTempView("staging_int_countries_final")

    # create staging_countries dataframe
    df_staging_countries = spark.sql("""
        SELECT 
        ic.int_country_code, ic.int_country_name,
        cc.country_code, cc.country_iso_code, NVL(cc.country_name, ic.int_country_name) country_name,
        cc.population, cc.area_km2, cc.gdp_usd_billion
        FROM staging_int_countries_final ic
        LEFT JOIN staging_country_codes cc on UPPER(ic.int_country_name) = UPPER(cc.country_name)
        ORDER BY int_country_name
    """)

    # create staging_countries temp-view
    df_staging_countries.createOrReplaceTempView("staging_countries")

    # create dim_countries dataframe
    df_dim_countries = spark.sql("""
        SELECT INT(int_country_code) country_key, INT(country_code), country_iso_code, country_name
        FROM staging_countries
    """)

    # create dim_countries temp-view
    df_dim_countries.createOrReplaceTempView("dim_countries")

    # write dim_countries table to parquet files
    df_dim_countries.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/dim_countries.parquet')


def process_temperature(spark):
    # read the GlobalLandTemperaturesByCountry data
    df_temperature = spark.read.csv(INPUT_PATH+"/GlobalLandTemperaturesByCountry.csv", header=True)

    # create temperature temp-view
    df_temperature.createOrReplaceTempView("temperature")

    # create staging_temperature dataframe
    df_staging_temperature = spark.sql("""
        SELECT tp.dt date, tp.AverageTemperature average_temperature, 
        tp.AverageTemperatureUncertainty average_temperature_uncertainty, 
        sc.country_name, sc.int_country_code, sc.country_code
        FROM temperature tp
        LEFT JOIN staging_countries sc ON UPPER(tp.Country) = UPPER(sc.country_name)
        WHERE country_name IS NOT NULL
        AND tp.AverageTemperature IS NOT NULL
    """)

    # create staging_temperature temp-view
    df_staging_temperature.createOrReplaceTempView("staging_temperature")

    # write staging_temperature table to parquet files
    df_staging_temperature.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/staging/staging_temperature.parquet')

    # create fact_temperature dataframe
    df_fact_temperature = spark.sql("""
        SELECT gen_uuid() id,
        date(date) date_key, 
        INT(int_country_code) country_key,
        DOUBLE(average_temperature), 
        DOUBLE(average_temperature_uncertainty)    
        FROM staging_temperature
        ORDER BY date
    """)

    # write fact_temperature table to parquet files
    df_fact_temperature.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/fact_temperature.parquet')


def process_airlines(spark):
    # read the airlines data
    df_airlines = spark.read.csv(INPUT_PATH+"/airlines.csv", header=True)

    # rename columns
    df_dim_airlines = df_airlines.withColumnRenamed("Code","airline_key") \
           .withColumnRenamed("Airline","airline_name") 

    # create dim_airlines temp-view
    df_dim_airlines.createOrReplaceTempView("dim_airlines")

    # write dim_airlines table to parquet files
    df_dim_airlines.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/dim_airlines.parquet')
    

def process_travel_modes(spark):
    # schema for travel_modes
    travel_modes_schema = StructType([
        StructField("travel_mode_key", IntegerType(), False),
        StructField("travel_mode_name", StringType(), True)
    ])

    # list of travel_modes
    travel_modes = [
        {"travel_mode_key": 1, "travel_mode_name": "Air"},
        {"travel_mode_key": 2, "travel_mode_name": "Sea"},
        {"travel_mode_key": 9, "travel_mode_name": "Not reported"}
    ]

    # create dim_travel_modes dataframe
    df_dim_travel_modes = spark.createDataFrame(travel_modes, schema=travel_modes_schema)

    # write dim_travel_modes table to parquet files
    df_dim_travel_modes.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/dim_travel_modes.parquet')


def process_visa_categories(spark):
    # schema for visa categories 
    visa_categories_schema = StructType([
        StructField("visa_category_key", IntegerType(), False),
        StructField("visa_category_name", StringType(), True)
    ])

    # list of visa_categories
    visa_categories = [
        {"visa_category_key": 1, "visa_category_name": "Business"},
        {"visa_category_key": 2, "visa_category_name": "Pleasure"},
        {"visa_category_key": 3, "visa_category_name": "Student"},
    ]
   
    # create dim_visa_categories dataframe
    df_dim_visa_categories = spark.createDataFrame(visa_categories, schema=visa_categories_schema)

    # write dim_visa_categories table to parquet files
    df_dim_visa_categories.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/dim_visa_categories.parquet')


def process_port_of_entry(spark):
    # read the port_of_entry data
    df_port_of_entry = spark.read.text(INPUT_PATH+"/port_of_entry.txt")

    # parse the internal_country_codes data
    dfParsed= df_port_of_entry.withColumn("parsed", parsePortOfEntryUDF("value"))
    fields = ["port_of_entry_code", "port_of_entry_name"]
    exprs = [ "parsed['{}'] as {}".format(field,field) for field in fields]
    dfClean = dfParsed.selectExpr(*exprs)

    # rename column
    df_dim_port_of_entry = dfClean.withColumnRenamed("port_of_entry_code","port_of_entry_key")

    # create dim_port_of_entry temp-view
    df_dim_port_of_entry.createOrReplaceTempView("dim_port_of_entry")

    # write dim_port_of_entry table to parquet files
    df_dim_port_of_entry.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/dim_port_of_entry.parquet')


def process_us_cities_demographics(spark):
    """
    Processes the us-cities-demographics.csv file and generates the facts and dimension tables listed below: 
    - fact_us_population
    - fact_us_race
    - dim_states
    """

    # schema for df_us_cities_demographics
    us_cities_demographics_schema = StructType([
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Median Age", DoubleType(), True), 
        StructField("Male Population", IntegerType(), True), 
        StructField("Female Population", IntegerType(), True), 
        StructField("Total Population", IntegerType(), True), 
        StructField("Number of Veterans", IntegerType(), True), 
        StructField("Foreign-born", IntegerType(), True), 
        StructField("Average Household Size", DoubleType(), True), 
        StructField("State Code", StringType(), True), 
        StructField("Race", StringType(), True), 
        StructField("Count", IntegerType(), True), 
    ])

    # read the us_cities_demographics data
    df_us_cities_demographics = spark.read.csv(INPUT_PATH+"/us-cities-demographics.csv", sep=";", header=True, schema=us_cities_demographics_schema)

    # rename columns
    df_us_cities_demographics = df_us_cities_demographics.withColumnRenamed("City","city") \
            .withColumnRenamed("State","state_name") \
            .withColumnRenamed("Median Age","median_age") \
            .withColumnRenamed("Male Population","male_population") \
            .withColumnRenamed("Female Population","female_population") \
            .withColumnRenamed("Total Population","total_population") \
            .withColumnRenamed("Number of Veterans","number_of_veterans") \
            .withColumnRenamed("Foreign-born","foreign_born") \
            .withColumnRenamed("Average Household Size","avg_household_size") \
            .withColumnRenamed("State Code","state_code") \
            .withColumnRenamed("Race","race") \
            .withColumnRenamed("Count","count")
    
    # create staging_us_cities_demographics temp-view
    df_us_cities_demographics.createOrReplaceTempView("staging_us_cities_demographics")

    # write staging_us_cities_demographics table to parquet files
    df_us_cities_demographics.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/staging/staging_us_cities_demographics.parquet')

    # create fact_us_population dataframe
    df_fact_us_population = spark.sql("""
        SELECT gen_uuid() id, tbl.* FROM (
            SELECT DISTINCT
            state_code state_key,
            city,
            median_age,
            male_population,
            female_population,
            total_population,
            number_of_veterans,
            foreign_born
            FROM staging_us_cities_demographics
        ) tbl
    """)

    # write fact_us_population table to parquet files
    df_fact_us_population.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/fact_us_population.parquet')

    # create fact_us_race dataframe
    df_fact_us_race = spark.sql("""
        SELECT gen_uuid() id, tbl.* FROM (
            SELECT DISTINCT
            state_code state_key,
            city,
            count
            FROM staging_us_cities_demographics
        ) tbl
    """)

    # write fact_us_race table to parquet files
    df_fact_us_race.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/fact_us_race.parquet')

    # create dim_states dataframe
    df_dim_states = spark.sql("""
        SELECT state_code state_key, state_name 
        FROM staging_us_cities_demographics
    """)

    # create dim_states temp-view
    df_dim_states.createOrReplaceTempView("dim_states")

    # write dim_states table to parquet files
    df_dim_states.write.mode("overwrite").\
        parquet(OUTPUT_PATH+'/dim_states.parquet')
    
    
def process_immigration(spark):
    # read the immigration data
    df_immigration = spark.read.format("parquet").load(INPUT_PATH+"/sas_data/*")

    # create immigration temp-view
    df_immigration.createOrReplaceTempView("immigration")

    # create fact_immigration dataframe
    df_fact_immigration = spark.sql("""
        SELECT INT(cicid) id,  
        INT(si.i94cit) country_citizen_key,
        INT(si.i94res) country_resident_key,
        STRING(si.i94port) port_of_entry_key,
        DATE(convert_sas_date(si.arrdate)) arrival_date_key,
        INT(si.i94mode) travel_mode_key,
        STRING(si.i94addr) state_key,
        DATE(convert_sas_date(si.depdate)) departure_date_key,
        INT(i94bir) age,
        INT(si.i94visa) visa_category_key,
        BOOLEAN(CASE
            WHEN si.matflag IS NULL THEN
                0
            ELSE
                1
            END) match_flag,
        STRING(gender),
        INT(insnum) ins_num,
        STRING(si.airline) airline_key,
        INT(admnum) admission_number,
        STRING(fltno) flight_number,
        STRING(visatype) visa_type,
        INT(i94yr) year,
        INT(i94mon) month
        FROM immigration si
    """)

    # create fact_immigration dataframe
    df_fact_immigration.createOrReplaceTempView("fact_immigration")

    # write fact_immigration data to parquet files partitioned by year and month
    df_fact_immigration.write.partitionBy("year", "month").mode("overwrite").\
        parquet(OUTPUT_PATH+"/fact_immigration.parquet")

    # create dim_dat dataframe
    df_dim_date = spark.sql("""
        SELECT date date_key,
        date, 
        extract(year from date) year,
        extract(quarter from date) quarter,
        extract(month from date) month, 
        extract(day from date) day, 
        extract(week from date) week
        FROM ( 
            (SELECT DISTINCT arrival_date_key date FROM fact_immigration
            WHERE arrival_date_key IS NOT NULL)
            UNION
            (SELECT DISTINCT departure_date_key date FROM fact_immigration
            WHERE departure_date_key IS NOT NULL)
        )
        ORDER BY date        
    """)

    # create dim_date temp-view
    df_dim_date.createOrReplaceTempView("dim_date")

    df_dim_date.write.mode("overwrite").\
        parquet(OUTPUT_PATH+"/dim_date.parquet")


def main():
    spark = create_spark_session()

    register_udfs(spark)    
    process_countries(spark)
    process_temperature(spark)
    process_airlines(spark)
    process_travel_modes(spark)
    process_visa_categories(spark)
    process_port_of_entry(spark)
    process_us_cities_demographics(spark)
    process_immigration(spark)
        

    print("Completed ETL")


if __name__ == "__main__":
    main()
