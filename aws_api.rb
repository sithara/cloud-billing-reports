require 'net/http'
require 'uri'
require 'aws-sdk'
require 'csv'
require 'active_record'
require 'bulk_insert'
require_relative './sql_connection'
require 'yaml'

class AwsEyBilling

  def self.fetch_report
begin
   billing_periods = [Time.now.to_date.prev_month.strftime('%Y-%m-01'), Time.now.strftime('%Y-%m-01')]

credential_file = YAML::load_file(File.join(File.dirname(File.expand_path(__FILE__)), 'credentials.yml'))

#billing_periods = ['2018-11-01']
#credential_file = YAML.load_file('credentials.yml')
credential_file['credentials']['ey_aws'].each do |k,v|

  billing_periods.each do |billing_period|

    end_date = (billing_period.to_date + 1.month).strftime('%Y-%m-01')

    cost_explorer = Aws::CostExplorer::Client.new(
      access_key_id: v['access_key'],
      secret_access_key: v['secret_access_key'],
      region: 'us-east-1'
    )

    a = cost_explorer.get_cost_and_usage({
      time_period: {
        start: billing_period, # required
        end: end_date, # required
      }, granularity: "DAILY", metrics: ["UnblendedCost"],
      group_by: [
      {
        type: "DIMENSION", # accepts DIMENSION, TAG
        key: "SERVICE",
      },
      {
        type: "DIMENSION", # accepts DIMENSION, TAG
        key: "INSTANCE_TYPE",
      },
    ]})

records = []
c = a.results_by_time.to_a.each do |x|
    x.groups.each do |record|
      records << [x.time_period.to_a.first,x.time_period.to_a.last, record.metrics["UnblendedCost"]["amount"],record["keys"].first, record["keys"].last, record.metrics["UnblendedCost"]["unit"], billing_period, 'EY_AWS', 'AWS', 1 ]
      #records << [x.time_period.to_a.first,x.time_period.to_a.last, record["keys"].first, record["keys"].last, record.metrics["UnblendedCost"]["amount"], record.metrics["UnblendedCost"]["unit"]]
    end
  end


  AwsEyReport.bulk_insert do |worker|
     records.each do |arr|
        worker.add usage_start_date: arr[0], usage_end_date: arr[1], amount: arr[2], instance_type: arr[3], service: arr[4], unit: arr[5], report_date: arr[6], data_source: arr[7], provider: arr[8], auto_generated: arr[9], linked_account_name: v['name']
   end
  end

  AwsEyReportTemp.bulk_insert do |worker|
     records.each do |arr|
        worker.add usage_start_date: arr[0], usage_end_date: arr[1], amount: arr[2], instance_type: arr[3], service: arr[4], unit: arr[5], report_date: arr[6], data_source: arr[7], provider: arr[8], auto_generated: arr[9], linked_account_name: v['name']
   end
  end

end
  end
rescue Exception => e

    File.open("aws_ey.txt","w") do |file|
      file.write(e.backtrace)
    end

end
  end
end


class AwsEyReport < AzureDatabase
  self.primary_key = 'id'
  self.table_name = 'aws_ey_reports'
end


class AwsEyReportTemp < AzureDatabase
  self.primary_key = 'id'
  self.table_name = 'aws_ey_reports_temp'
end


AwsEyBilling.fetch_report
