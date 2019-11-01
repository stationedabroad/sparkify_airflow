# sparkify_airflow
This folder includes contents on a basic implementation project for airflow, using a pipeline which serves using the following:
  * **S3 connector source data reads json from S3 buckets**
  * **AWS Redshift connector target writes to staging and fact/dimension tables**
  * **Basic automation of data quality checks**

The basic premise aims to carry input source data written to S3 buckets as json files, on a fixed schedule to an AWS Redshift staging area.  From here data is used to populate Fact and Dimension tables to satisfy a star schema pattern of data-warehouse design.
