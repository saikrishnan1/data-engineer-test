import time
import logging
import glob,os,json
from pyspark.sql import SparkSession # type: ignore
#from pyspark.sql.functions import col, lower, trim, row_number
#from pyspark.sql.window import Window
from pyspark.sql.functions import  col,udf,regexp_replace, lower, trim
from pyspark.sql.types import  StringType

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure the logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(base_dir,'spark_implementation','logging','job.log')),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger()
 
def create_country_mapping_udf(mapping_path):
    """Creates a UDF for country mapping."""
    with open(mapping_path, 'r') as f:
        country_mapping = json.load(f)
    
    def country_map(noc):
        return country_mapping.get(noc, "Unknown")
    
    return udf(country_map, StringType())

def format_country_name(name):
    """Formats a country name by converting to lowercase, removing special characters, and trimming whitespace."""
    return regexp_replace(lower(trim(name)), r'[^a-zA-Z\s]', '')


def main():
    """Main ETL script definition.

    :return: None
    """
    try:
        # start Spark application and get Spark session, logger and config
        spark = SparkSession.builder.master('local[*]').appName('holman_de_test').getOrCreate()
        
        
        # capture execution time
        # Start timing
        start_time = time.time()
        #Define absolute paths
        olympics_data_path = os.path.join(base_dir, 'datasets', 'olympics')
        countries_path=os.path.join(base_dir,'datasets','countries','countries of the world.csv')
        noc_countries_mapping_path = os.path.join(base_dir, 'datasets', 'noc_countries.json')
        
        #output write paths
        olympics_write_path=os.path.join(base_dir, 'output_files', 'spark_olympics_combined.csv')
        merged_write_path=os.path.join(base_dir, 'output_files', 'merged_output.csv')
        
        # Load Olympics data
        # Ingest all csv files into one dataframe
        logger.info("\nLoading Olympics data...")
        olympics_df = spark.read.csv(f'{olympics_data_path}/*.csv', header=True, inferSchema=True)
        logger.info("Total Record Count:" +  str(olympics_df.count()))
        
        # Load Countries data
        logger.info("\nLoading Countries data....")
        countries_df = spark.read.csv(f'{countries_path}', header=True, inferSchema=True)
        logger.info("Total Record Count:" +  str(countries_df.count()))
        #transformation1
        countries_df = countries_df.withColumn("country", format_country_name(countries_df["Country"]))
        #countries_df.show()
        #transformation2
        #We use a User-Defined Function (UDF) to apply the mapping logic to each row of the DataFrame
        country_mapping_udf = create_country_mapping_udf(noc_countries_mapping_path) 
        #Adds Normalization by adding the foreign key
        olympics_df = olympics_df.withColumn("country", country_mapping_udf(col("NOC")))
        #olympics_df.show()
        
        #Denormalization
        # Join the Olympics and Countries DataFrames
        logger.info("\nJoining Olympics and Countries data...")
        merged_df = olympics_df.join(countries_df, on="country", how="right")
        logger.info("Total Record Count:" +  str(merged_df.count()))
        
         # Save the joined Olympics and Countries DataFrame to CSV and Parquet
        logger.info("\nSaving joined Olympics and Countries data to CSV and Parquet.")
        olympics_df.write.csv(f'{olympics_write_path}', header=True, mode='overwrite')
        merged_df.write.csv(f'{merged_write_path}', header=True, mode='overwrite')
        #parquet is optional
        #olympics_df.write.parquet('output_files/olympics_countries_combined.parquet', mode='overwrite')
        logger.info("Joined Olympics and Countries data saved.")

        

        # End timing
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Pipeline executed in {execution_time:.2f} seconds")
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()