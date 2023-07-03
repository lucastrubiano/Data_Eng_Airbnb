
-- show create table default.calendar_csv;
/*
CREATE TABLE hive_metastore.default.calendar_csv (
  listing_id STRING,
  date STRING,
  available STRING,
  price STRING
) USING delta TBLPROPERTIES (
  'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2'
)
*/

-- show create table default.listings_csv;
/*
CREATE TABLE hive_metastore.default.listings_csv (
  id STRING,
  listing_url STRING,
  scrape_id STRING,
  last_scraped STRING,
  source STRING,
  name STRING,
  description STRING,
  neighborhood_overview STRING,
  picture_url STRING,
  host_id STRING,
  host_url STRING,
  host_name STRING,
  host_since STRING,
  host_location STRING,
  host_about STRING,
  host_response_time STRING,
  host_response_rate STRING,
  host_acceptance_rate STRING,
  host_is_superhost STRING,
  host_thumbnail_url STRING,
  host_picture_url STRING,
  host_neighbourhood STRING,
  host_listings_count STRING,
  host_total_listings_count STRING,
  host_verifications STRING,
  host_has_profile_pic STRING,
  host_identity_verified STRING,
  neighbourhood STRING,
  neighbourhood_cleansed STRING,
  neighbourhood_group_cleansed STRING,
  latitude STRING,
  longitude STRING,
  property_type STRING,
  room_type STRING,
  accommodates STRING,
  bathrooms STRING,
  bathrooms_text STRING,
  bedrooms STRING,
  beds STRING,
  amenities STRING,
  price STRING,
  minimum_nights STRING,
  maximum_nights STRING,
  minimum_minimum_nights STRING,
  maximum_minimum_nights STRING,
  minimum_maximum_nights STRING,
  maximum_maximum_nights STRING,
  minimum_nights_avg_ntm STRING,
  maximum_nights_avg_ntm STRING,
  calendar_updated STRING,
  has_availability STRING,
  availability_30 STRING,
  availability_60 STRING,
  availability_90 STRING,
  availability_365 STRING,
  calendar_last_scraped STRING,
  number_of_reviews STRING,
  number_of_reviews_ltm STRING,
  number_of_reviews_l30d STRING,
  first_review STRING,
  last_review STRING,
  review_scores_rating STRING,
  review_scores_accuracy STRING,
  review_scores_cleanliness STRING,
  review_scores_checkin STRING,
  review_scores_communication STRING,
  review_scores_location STRING,
  review_scores_value STRING,
  license STRING,
  instant_bookable STRING,
  calculated_host_listings_count STRING,
  calculated_host_listings_count_entire_homes STRING,
  calculated_host_listings_count_private_rooms STRING,
  calculated_host_listings_count_shared_rooms STRING,
  reviews_per_month STRING
) USING delta TBLPROPERTIES (
  'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2'
)
*/

-- show create table default.listings_summary_csv;
/*
CREATE TABLE hive_metastore.default.listings_summary_csv (
  id STRING,
  name STRING,
  host_id STRING,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude STRING,
  longitude STRING,
  room_type STRING,
  price STRING,
  minimum_nights STRING,
  number_of_reviews STRING,
  last_review STRING,
  reviews_per_month STRING,
  calculated_host_listings_count STRING,
  availability_365 STRING,
  number_of_reviews_ltm STRING,
  license STRING
) USING delta TBLPROPERTIES (
  'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2'
)
*/

-- show create table default.neighbourhoods_csv;
/*
CREATE TABLE hive_metastore.default.neighbourhoods_csv (
  neighbourhood_group STRING,
  neighbourhood STRING
) USING delta TBLPROPERTIES (
  'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2'
)
*/

-- show create table default.reviews_csv;
/*
CREATE TABLE hive_metastore.default.reviews_csv (
  listing_id STRING,
  id STRING,
  date STRING,
  reviewer_id STRING,
  reviewer_name STRING,
  comments STRING
) USING delta TBLPROPERTIES (
  'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2'
)
*/

-- show create table default.reviews_summary_csv;
/*
CREATE TABLE hive_metastore.default.reviews_summary_csv (
  listing_id STRING,
  date STRING
) USING delta TBLPROPERTIES (
  'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2'
)
*/

-- For staging

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.calendar (
  listing_id STRING,
  date STRING,
  available STRING,
  price STRING
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/staging/calendar';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.listings (
  id STRING,
  listing_url STRING,
  scrape_id STRING,
  last_scraped STRING,
  source STRING,
  name STRING,
  description STRING,
  neighborhood_overview STRING,
  picture_url STRING,
  host_id STRING,
  host_url STRING,
  host_name STRING,
  host_since STRING,
  host_location STRING,
  host_about STRING,
  host_response_time STRING,
  host_response_rate STRING,
  host_acceptance_rate STRING,
  host_is_superhost STRING,
  host_thumbnail_url STRING,
  host_picture_url STRING,
  host_neighbourhood STRING,
  host_listings_count STRING,
  host_total_listings_count STRING,
  host_verifications STRING,
  host_has_profile_pic STRING,
  host_identity_verified STRING,
  neighbourhood STRING,
  neighbourhood_cleansed STRING,
  neighbourhood_group_cleansed STRING,
  latitude STRING,
  longitude STRING,
  property_type STRING,
  room_type STRING,
  accommodates STRING,
  bathrooms STRING,
  bathrooms_text STRING,
  bedrooms STRING,
  beds STRING,
  amenities STRING,
  price STRING,
  minimum_nights STRING,
  maximum_nights STRING,
  minimum_minimum_nights STRING,
  maximum_minimum_nights STRING,
  minimum_maximum_nights STRING,
  maximum_maximum_nights STRING,
  minimum_nights_avg_ntm STRING,
  maximum_nights_avg_ntm STRING,
  calendar_updated STRING,
  has_availability STRING,
  availability_30 STRING,
  availability_60 STRING,
  availability_90 STRING,
  availability_365 STRING,
  calendar_last_scraped STRING,
  number_of_reviews STRING,
  number_of_reviews_ltm STRING,
  number_of_reviews_l30d STRING,
  first_review STRING,
  last_review STRING,
  review_scores_rating STRING,
  review_scores_accuracy STRING,
  review_scores_cleanliness STRING,
  review_scores_checkin STRING,
  review_scores_communication STRING,
  review_scores_location STRING,
  review_scores_value STRING,
  license STRING,
  instant_bookable STRING,
  calculated_host_listings_count STRING,
  calculated_host_listings_count_entire_homes STRING,
  calculated_host_listings_count_private_rooms STRING,
  calculated_host_listings_count_shared_rooms STRING,
  reviews_per_month STRING
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/staging/listings';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.listings_summary (
  id STRING,
  name STRING,
  host_id STRING,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude STRING,
  longitude STRING,
  room_type STRING,
  price STRING,
  minimum_nights STRING,
  number_of_reviews STRING,
  last_review STRING,
  reviews_per_month STRING,
  calculated_host_listings_count STRING,
  availability_365 STRING,
  number_of_reviews_ltm STRING,
  license STRING
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/staging/listings_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.neighbourhoods (
  neighbourhood_group STRING,
  neighbourhood STRING
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/staging/neighbourhoods';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.reviews (
  listing_id STRING,
  id STRING,
  date STRING,
  reviewer_id STRING,
  reviewer_name STRING,
  comments STRING
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/staging/reviews';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.reviews_summary (
  listing_id STRING,
  date STRING
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/staging/reviews_summary';

-- For business

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.apartments_per_zone (
  neighbourhood STRING,
  total_apartments BIGINT
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/business/apartments_per_zone';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.reviews_per_year (
  year STRING,
  total_reviews BIGINT
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/business/reviews_per_year';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.word_frequency_paris (
  word STRING,
  frequency BIGINT
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/business/word_frequency_paris';

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.price_increase (
  listing_id BIGINT,
  price_increase_percentage BIGINT
) PARTITIONED BY (
  partition_date STRING
) STORED AS PARQUET LOCATION 'dbfs:/FileStore/tables/bi_corp/business/price_increase';