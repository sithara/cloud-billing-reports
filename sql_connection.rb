require 'active_record'

class AzureDatabase < ActiveRecord::Base
  self.abstract_class = true
  establish_connection(adapter: 'sqlserver', host: "your-host", username: "enter-your-username", password: "enter-password", database: "your-database", azure: true, port: 1433,checkout_timeout: 10)
end
