# Azure and AWS Cloud Usage Reports

Cloud Billing Reports helps users consolidate billing reports pulled in from Azure and AWS , grouped by engagement. 

## Requirements

Enable Azure Usage Reports to obtain an api key, refer https://docs.microsoft.com/en-us/azure/billing/billing-enterprise-api on how to generate the api key . AWS usage report can either be pulled from its api or from S3 bucket https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/billing-reports-gettingstarted-s3.html

## Azure Usage Report

Set the authorization key at https://github.com/sithara/cloud-billing-reports/blob/master/azure.rb#L20 and run the script to obtain azure usage reports for the last 2 months. The is script saves report to azure_billing_report and azure_billing_reporte 

## AWS Usage Report

If you have set up an aws s3 bucket to receive usage report, configure aws https://github.com/sithara/cloud-billing-reports/blob/master/aws.rb#L60-L64 and specify the bucket name in https://github.com/sithara/cloud-billing-reports/blob/master/aws.rb#L18

api 

bucket aws_reports, aws_reports_temp


## Consolidate Azure and AWS Billing Report

Create master and temp tables for azure and aws. The master tables has historic data whereas the temp tables maintain previous two months data. The temp tables are created to improve performance of the scripts.


Run the `ruby infra_report.rb` script to consolidate the aws and azure reports and save it to staging table INFRASTRUCTURE_REPORT_FINAL. The script also populates the OUT_INFRA_REPORT table, customized report


SUBSCRIPTION_ENGAGEMENT_MAP
OUT_INFRA_REPORT
