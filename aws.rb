require 'net/http'
require 'uri'
require 'aws-sdk'
require 'csv'
require 'active_record'
require 'bulk_insert'
require_relative './sql_connection'

class AwsBilling

  def self.fetch_report
  begin
     billing_periods = [Time.now.to_date.prev_month.strftime('%Y-%m'), Time.now.strftime('%Y-%m')]

     billing_periods.each do |duration|

    File.open("aws.csv", 'wb') do |file|
        Aws::S3::Client.new.get_object(bucket: "eyc3billing", key: 'your-key-'+duration+'.csv') do |chunk|
         file.write(chunk)
       end
    end

    my_array = []
    counter = 0
    File.open("aws.csv").each do |line| # `foreach` instead of `open..each` does the same
      begin
        CSV.parse(line) do |row|
          my_array << row if row[3] == "LinkedLineItem"
        end
       rescue CSV::MalformedCSVError => er
        counter += 1
        next
      end
      counter += 1
    end


  AwsReport.bulk_insert do |worker|
     my_array.each do |arr|
               worker.add report_date: DateTime.strptime(duration+'-01','%Y-%m-%d'),linked_account_id: arr[2], invoice_date: arr[7], billing_period_start_date: arr[5], billing_period_end_date: arr[6], payer_account_name: arr[8], payer_po_number: arr[11], product_code: arr[12], usage_type: arr[15], operation: arr[16], item_description: arr[18], usage_start_date: arr[19], usage_end_date: arr[20], usage_quantity: arr[21], blended_rate: arr[22], currency_code: arr[23], cost_before_tax: arr[24], credits: arr[25], tax_amount: arr[26], tax_type: arr[27], total_cost: arr[28], availability_zone: "", seller_of_record: arr[14], product_name: arr[13], taxation_address: arr[10], linked_account_name: arr[9], record_type: arr[3], record_id: arr[4], rate_id: arr[17], invoice_id: arr[0], payer_account_id: arr[1]
     end
   end

  AwsReportTemp.bulk_insert do |worker|
     my_array.each do |arr|
        worker.add report_date: DateTime.strptime(duration+'-01','%Y-%m-%d'),linked_account_id: arr[2], invoice_date: arr[7], billing_period_start_date: arr[5], billing_period_end_date: arr[6], payer_account_name: arr[8], payer_po_number: arr[11], product_code: arr[12], usage_type: arr[15], operation: arr[16], item_description: arr[18], usage_start_date: arr[19], usage_end_date: arr[20], usage_quantity: arr[21], blended_rate: arr[22], currency_code: arr[23], cost_before_tax: arr[24], credits: arr[25], tax_amount: arr[26], tax_type: arr[27], total_cost: arr[28], availability_zone: "", seller_of_record: arr[14], product_name: arr[13], taxation_address: arr[10], linked_account_name: arr[9], record_type: arr[3], record_id: arr[4], rate_id: arr[17], invoice_id: arr[0], payer_account_id: arr[1]
      end
    end
  end
end
  rescue Exception => e
   p e.message
    File.open("aws.txt","w") do |file|
      file.write(e.backtrace)
    end
  ensure
    AwsReport.connection.close
  end
end

Aws.config.update(
        access_key_id: 'your_access_key',
        secret_access_key: 'your_secret_access',
        region: 'your_region'
)

class AwsReport < AzureDatabase
  self.primary_key = 'id'
  self.table_name = 'aws_reports'
end


class AwsReportTemp < AzureDatabase
  self.primary_key = 'id'
  self.table_name = 'aws_reports_temp'
end

AwsBilling.fetch_report
