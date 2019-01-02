require 'active_record'

ActiveRecord::Base.establish_connection adapter: 'sqlserver', host: "your_host", username: "your_username", password: "your_password", database: "your_database", azure: true, port: 1433, pool: 5, timeout: 2500000


create_infra_report = "INSERT into [dbo].[INFRASTRUCTURE_REPORT_FINAL] (ReportDate, LinkedAccountName, AccoutName, ProductCode, MeterSubcategory, TotalCost, Source,InstanceID)
select  report_date AS ReportDate, subscription_name AS LinkedAccountName,
subscription_name AS AccoutName,meter_category AS ProductCode, meter_subcategory AS MeterSubcategory,
extended_cost AS TotalCost, source AS Source, instance_id AS InstanceID
from azure_billing_report_temp where subscription_name != 'Client Environments - Small' -- Exclude BIL case
UNION ALL
-- TDM Client small case post '2017-09-01'
select  report_date AS ReportDate, 'Client Environments - Small TDM' AS LinkedAccountName, --, [resource_group],
    'Client Environments - Small TDM' AS AccoutName, meter_category AS ProductCode, meter_subcategory AS MeterSubcategory,
    extended_cost AS TotalCost, source AS Source, instance_id AS InstanceID
    from azure_billing_report_temp where subscription_name = 'Client Environments - Small'
    and ((UPPER([resource_group]) like 'TDMRG01%')
            or ([resource_group] in ('bilamld001-Workspace','bilamld001-WorkspaceSeat','BILDEVRG001','BILDEVRG002',
                                        'bileuroamld001-Workspace','bileuroamld001-WorkspaceSeat','bileuroamlp001-Workspace',
                                                         'bileuroamlp001-WorkspaceSeat','BILEuroDevRG001','Default-AADDomainServices-AustraliaEast','securitydata',''))
              )
 UNION ALL -- add metro
  select  report_date AS ReportDate, 'Client Environments - Metro Trains' AS LinkedAccountName, --, [resource_group],
    'Client Environments - Metro Trains' AS AccoutName, meter_category AS ProductCode, meter_subcategory AS MeterSubcategory,
    extended_cost AS TotalCost, source AS Source, instance_id AS InstanceID
    from azure_billing_report_temp where subscription_name = 'Client Environments - Small'
    and (UPPER([resource_group]) like 'Metro%')
       UNION ALL
Select  report_date AS ReportDate, CAST(linked_account_name AS NVARCHAR(MAX)) AS LinkedAccountName,
CAST(linked_account_name AS NVARCHAR(MAX)) AS AccountName, CAST(product_name AS NVARCHAR(MAX)) AS ProductCode, '' AS MeterSubcategory,
(total_cost*1.37) AS TotalCost, CAST(provider AS NVARCHAR(MAX)) AS Source, '' AS InstanceID
  from aws_reports_temp
 UNION ALL Select  report_date AS ReportDate, CAST(linked_account_name AS NVARCHAR(MAX)) AS LinkedAccountName,
CAST(linked_account_name AS NVARCHAR(MAX)) AS AccountName, CAST(instance_type AS NVARCHAR(MAX)) AS ProductCode, [service] AS MeterSubcategory,
(amount/0.78435) AS TotalCost, CAST(provider AS NVARCHAR(MAX)) AS Source, '' AS InstanceID
  from aws_ey_reports_temp"

delete_prev_records = "DELETE from OUT_INFRA_REPORT WHERE ReportDate > '#{(Time.now - 2.months).strftime('%Y-%m-01')}'"

update_out_infra_report_final = "INSERT INTO OUT_INFRA_REPORT
select *, SYSDATETIME() [Creation_date] from

(
SELECT DISTINCT [ReportDate]
      ,[LinkedAccountName]
      ,[AccoutName]
      ,[ProductCode]
      ,[MeterSubcategory]
      ,UPPER([Source]) source
      --,[InstanceID]
         , ISNULL(b.engagement,'EYC3 Managed Services') engagement
      ,SUM([TotalCost]) Costs
  FROM [dbo].[INFRASTRUCTURE_REPORT_FINAL] a
  left outer join ( select distinct [subscription_name], [engagement] ,[INFRA_TYPE]  from [dbo].[SUBSCRIPTION_ENGAGEMENT_MAP]) b
                  on ( a.AccoutName = b.[subscription_name] and
                                  UPPER(a.Source) = UPPER(b.INFRA_TYPE))
  Group by
  [ReportDate]
      ,[LinkedAccountName]
      ,[AccoutName]
      ,[ProductCode]
      ,[MeterSubcategory]
      ,[Source]
     -- ,[InstanceID]
         , b.engagement) a WHERE a.Costs > 0"

update_report_sql = "update OUT_INFRA_REPORT set [MeterSubcategory]=[ProductCode] where meterSubcategory = ''"

insert_report_sql  = "insert into [dbo].[INP_ENG_RATES] (GPN, [Employee name], [activity code name],[activity code],[Eng_rate (AUD Per hour)])
select distinct GPN, [Employee name], [activity code name],[activity code number], NULL
from [dbo].[INP_MS_ACTUALS] a
where GPN is not null
and not exists
(select 1 from [dbo].[INP_ENG_RATES] y
where a.gpn = y.gpn
and a.[activity code name] = y.[activity code name]
and a.[activity code number] = y.[activity code] )"
insert_activity_code = "insert into [dbo].[MAP_ACTIVITY_ENGAGEMENT] ([Engagement_id],[Engagement],[activity_code],[activity code name])
select distinct 'ENG0015','EYC3 Managed Services', [activity code number],[activity code name]
from [dbo].[INP_MS_ACTUALS] a
where not exists
( select 1 from [dbo].[MAP_ACTIVITY_ENGAGEMENT] y
  where a.[activity code name] = y.[activity code name]
  and a.[activity code number] = y.[activity_code] )
and a.[activity code number] is not null"

ActiveRecord::Base.connection.execute(create_infra_report)
ActiveRecord::Base.connection.execute(delete_prev_records)
ActiveRecord::Base.connection.execute(update_out_infra_report_final)
ActiveRecord::Base.connection.execute(update_report_sql)
ActiveRecord::Base.connection.execute(insert_report_sql)
ActiveRecord::Base.connection.execute(insert_activity_code)
