require 'active_record'

conn = ActiveRecord::Base.establish_connection adapter: 'sqlserver', host: "your-host", username: "your-username", password: "your-password", database: "your-database", azure: true, port: 1433, timeout: 2500000

delete_azure_report_sql = "DELETE  FROM azure_billing_report where report_date > '#{(Time.now - 2.months).strftime('%Y-%m-01')}'"

delete_aws_report_sql = "DELETE FROM aws_reports where report_date > '#{(Time.now - 2.months).strftime('%Y-%m-01')}'"

ActiveRecord::Base.connection.raw_connection.execute(delete_azure_report_sql).do
ActiveRecord::Base.connection.raw_connection.execute(delete_aws_report_sql).do
ActiveRecord::Base.connection.raw_connection.execute("Truncate table azure_billing_report_temp").do
ActiveRecord::Base.connection.raw_connection.execute("Truncate table aws_reports_temp").do
ActiveRecord::Base.connection.raw_connection.execute("Truncate table INFRASTRUCTURE_REPORT_FINAL").do
ActiveRecord::Base.remove_connection(conn)
