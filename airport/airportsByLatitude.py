
from pyspark import SparkContext,SparkConf
import re

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line: str):
    splits = COMMA_DELIMITER.split(line)
    return "{}, {},{}".format(splits[1], splits[6],splits[7])

if __name__ == "__main__":
    print("ho")

    conf = SparkConf().setAppName("airports1").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    air_rdd = sc.textFile("../datasets/airports.text")

    airportsByLatLong=  air_rdd.map(splitComma)
    airportsByLatLong.saveAsTextFile("../output/airports_lat_long.text")

    airportsByLat = air_rdd.filter(lambda line: float(COMMA_DELIMITER.split(line)[6]) >50)

    airportsByLatLong.saveAsTextFile("../output/airports_lat_gth_50")

