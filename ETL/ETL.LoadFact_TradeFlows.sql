set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_TradeFlows;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/12/2019
Description:	Creates the LoadFact_TradeFlows stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_TradeFlows
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_TradeFlows', 'U') is not null
		truncate table Staging.Fact_TradeFlows;

	begin try
		insert
				Staging.Fact_TradeFlows with (tablock)
		select
			distinct
				tf.QBRecId									TradeFlowAlternateKey,
				isnull(rc.CountryKey, -1)					ReportingCountryKey,
				isnull(pc.CountryKey, -1)					PartnerCountryKey,
				isnull(product.ProductKey, -1)				ProductKey,
				isnull(rd.DateKey, 18991230)				ReportDateKey,
				tf.ImportExport								ImportExport,
				tf.Unit										Unit,
				tf.Quantity									Quantity,
				tf.[Value]									[Value],
				tf.AveragePrice								AveragePrice
			from
				TradeFlows tf with (nolock)
					left join Warehouse.Dim_Product product with (nolock)
						on product.ProductAlternateKey = tf.RelatedVaultProductsID
					left join Warehouse.Dim_Country rc with (nolock)
						on rc.CountryAlternateKey = tf.RelatedReportingCountryID
					left join Warehouse.Dim_Country pc with (nolock)
						on pc.CountryAlternateKey = tf.RelatedPartnerCountryID
					left join Warehouse.Dim_Calendar rd with (nolock)
						on rd.FullDate = convert(date, tf.ReportDate);
	end try
	begin catch
		select @ErrorMsg = 'Staging TradeFlows records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_TradeFlows', 'U') is not null
		truncate table Warehouse.Fact_TradeFlows;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_TradeFlows with (tablock)
			select
					stf.TradeFlowAlternateKey,
					stf.ReportingCountryKey,
					stf.PartnerCountryKey,
					stf.ProductKey,
					stf.ReportDateKey,
					stf.ImportExport,
					stf.Unit,
					stf.Quantity,
					stf.[Value],
					stf.AveragePrice,
					getdate() RowStartDate
				from
					Staging.Fact_TradeFlows stf;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end