require 'net/http'
require 'uri'
require 'csv'
require 'date'
require 'active_record'
require 'bulk_insert'
require_relative './sql_connection'

class Azure

  def self.generate_report

    begin
      billing_periods = [Time.now.to_date.prev_month.strftime('%m-%Y'), Time.now.strftime('%m-%Y')]

      billing_periods.each do |duration|

        uri = URI.parse("https://ea.azure.com/rest/your-enrollment-number/usage-report?month=#{duration}&type=detail")
        request = Net::HTTP::Get.new(uri)
        request["Authorization"] = "Bearer your-secret-key"

        req_options = {
          use_ssl: uri.scheme == "https",
        }

        response = Net::HTTP.start(uri.hostname, uri.port, req_options) do |http|
          http.read_timeout = 1000
          http.request(request)
        end

        filename = "azure_monthly_usage_"+duration+".csv"

        file = File.open(filename, "w")
        file.write(response.body.force_encoding('utf-8'))

        my_array = []
        counter = 0

        File.open(file.path,encoding: 'utf-8').drop(1).each do |line| # `foreach` instead of `open..each` does the same
          begin
            CSV.parse(line) do |row|
              row[21] = row[21].split('/').last unless row.compact.reject(&:empty?).empty? || row[21].empty?
              my_array << row unless row[0].nil?
            end
          rescue CSV::MalformedCSVError => er
            counter += 1
            next
          end
          counter += 1
        end

        AzureBillingReport.bulk_insert do |worker|
          my_array.drop(2).each do |arr|
            worker.add report_date: Date.parse('01-'+duration).strftime('%Y-%m-%d'),account_owner_id: arr[0],account_name: arr[1],subscription_id: arr[3], subscription_guid: arr[4], subscription_name: arr[5], billing_date: arr[6], product: arr[10], meter: arr[11], meter_category: arr[12], meter_subcategory: arr[13], meter_region: arr[14], meter_name: arr[15], consumed_quantity: arr[16], resource_rate: arr[17], extended_cost: arr[18], resource_location: arr[19], consumer_service: arr[20],instance_id: arr[21], service_info_1: arr[22], service_info_2: arr[23], department: arr[27], cost_center: arr[28], unit_of_measure: arr[29], resource_group: arr[30],source: 'AZURE', data_source: 'AZURE', auto_generated: 1
          end
        end

        AzureReport.bulk_insert do |worker|
          my_array.drop(2).each do |arr|
            worker.add report_date: Date.parse('01-'+duration).strftime('%Y-%m-%d'),account_owner_id: arr[0],account_name: arr[1],subscription_id: arr[3], subscription_guid: arr[4], subscription_name: arr[5], billing_date: arr[6], product: arr[10], meter: arr[11], meter_category: arr[12], meter_subcategory: arr[13], meter_region: arr[14], meter_name: arr[15], consumed_quantity: arr[16], resource_rate: arr[17], extended_cost: arr[18], resource_location: arr[19], consumer_service: arr[20],instance_id: arr[21], service_info_1: arr[22], service_info_2: arr[23], department: arr[27], cost_center: arr[28], unit_of_measure: arr[29], resource_group: arr[30], source: 'AZURE', data_source: 'AZURE', auto_generated: 1
          end
        end

        file.close
        File.delete(filename)
      end

    rescue Exception => e
      File.open('azure.txt',"w") do |file|
        p "Something went wrong"
        file.write(e.backtrace)
      end
    ensure
      AzureReport.connection.close
    end

  end
end


class AzureReport < AzureDatabase
  self.table_name = 'azure_billing_report_temp'
  self.primary_key = 'id'
end

class AzureBillingReport < AzureDatabase
  self.table_name = 'azure_billing_report'
  self.primary_key = 'id'
end

Azure.generate_report
