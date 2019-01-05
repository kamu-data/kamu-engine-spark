import org.apache.spark.sql.types._

import scala.collection.mutable

object Schemas {
  val schemas = mutable.HashMap(
    "geoname" -> StructType(Array(
      StructField("geonameid", LongType, false),           // integer id of record in geonames database
      StructField("name", StringType, false),              // name of geographical point (utf8) varchar(200)
      StructField("asciiname", StringType, false),         // name of geographical point in plain ascii characters, varchar(200)
      StructField("alternatenames", StringType, true),     // alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
      StructField("latitude", DoubleType, false),          // latitude in decimal degrees (wgs84)
      StructField("longitude", DoubleType, false),         // longitude in decimal degrees (wgs84)
      StructField("feature_class", StringType, true),      // see http://www.geonames.org/export/codes.html, char(1)
      StructField("feature_code", StringType, true),       // see http://www.geonames.org/export/codes.html, varchar(10)
      StructField("country_code", StringType, true),       // ISO-3166 2-letter country code, 2 characters
      StructField("cc2", StringType, true),                // alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
      StructField("admin1_code", StringType, true),        // fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
      StructField("admin2_code", StringType, true),        // code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
      StructField("admin3_code", StringType, true),        // code for third level administrative division, varchar(20)
      StructField("admin4_code", StringType, true),        // code for fourth level administrative division, varchar(20)
      StructField("population", LongType, false),          // bigint (8 byte int)
      StructField("elevation", DoubleType, true),          // in meters, integer
      StructField("dem", StringType, true),                // digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
      StructField("timezone", StringType, true),           // the iana timezone id (see file timeZone.txt) varchar(40)
      StructField("modification_date", StringType, true)   // date of last modification in yyyy-MM-dd format
    )),
    "alternateNames" -> StructType(Array(
      StructField("alternateNameId", LongType, false),     // the id of this alternate name, int
      StructField("geonameid", LongType, false),           // geonameId referring to id in table 'geoname', int
      StructField("isolanguage", StringType, false),        // iso 639 language code 2- or 3-characters; 4-characters 'post' for postal codes and 'iata','icao' and faac for airport codes, fr_1793 for French Revolution names,  abbr for abbreviation, link to a website (mostly to wikipedia), wkdt for the wikidataid, varchar(7)
      StructField("alternateName", StringType, false),     // alternate name or name variant, varchar(400)
      StructField("isPreferredName", IntegerType, true),   // '1', if this alternate name is an official/preferred name
      StructField("isShortName", IntegerType, true),       // '1', if this is a short name like 'California' for 'State of California'
      StructField("isColloquial", IntegerType, true),      // '1', if this alternate name is a colloquial or slang term. Example: 'Big Apple' for 'New York'.
      StructField("isHistoric", IntegerType, true),        // '1', if this alternate name is historic and was used in the past. Example 'Bombay' for 'Mumbai'.
      StructField("from", StringType, true),               // from period when the name was used
      StructField("to", StringType, true)                  // to period when the name was used
    )),
    "location" -> StructType(Array(
      StructField("t", TimestampType, false),
      StructField("place", StringType, false),
      StructField("country", StringType, false),
      StructField("transport", StringType, false)
    ))
  )
}
