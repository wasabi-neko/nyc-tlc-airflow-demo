-- Create integration with AWS
CREATE or REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = ''
  STORAGE_ALLOWED_LOCATIONS = ('*');

DESC INTEGRATION s3_integration;


-- Create External Stage

create DATABASE nyc_tlc;
use nyc_tlc.public;
CREATE or REPLACE STAGE s3_stage
    URL = 's3://nyc-tlc-demo'
    STORAGE_INTEGRATION = s3_integration;
list @s3_stage;
