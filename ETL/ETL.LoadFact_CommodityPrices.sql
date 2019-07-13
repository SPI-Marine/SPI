set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_CommodityPrices;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/13/2019
Description:	Creates the LoadFact_CommodityPrices stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_CommodityPrices
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_CommodityPrices', 'U') is not null
		truncate table Staging.Fact_CommodityPrices;

	begin try
		insert
				Staging.Fact_CommodityPrices with (tablock)
		select
			distinct
				cp.QBRecId									CommodityPriceAlternateKey,
				isnull(product.ProductKey, -1)				ProductKey,
				isnull(rd.DateKey, 18991230)				ReportDateKey,
				cp.AssessmentType							AssessmentType,
				cp.Unit										Unit,
				cp.Remarks									Remarks,
				cp.PriceHigh								PriceHigh,
				cp.PriceLow									PriceLow,
				cp.PriceAverage								PriceAverage
			from
				CommodityPrices cp with (nolock)
					left join Warehouse.Dim_Product product with (nolock)
						on product.ProductAlternateKey = cp.RelatedProductID
					left join Warehouse.Dim_Calendar rd with (nolock)
						on rd.FullDate = convert(date, cp.ReportDate);
	end try
	begin catch
		select @ErrorMsg = 'Staging CommodityPrices records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_CommodityPrices', 'U') is not null
		truncate table Warehouse.Fact_CommodityPrices;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_CommodityPrices with (tablock)
			select
					scp.CommodityPriceAlternateKey,
					scp.ProductKey,
					scp.ReportDateKey,
					scp.AssessmentType,
					scp.Unit,
					scp.Remarks,
					scp.PriceHigh,
					scp.PriceLow,
					scp.PriceAverage,
					getdate() RowStartDate
				from
					Staging.Fact_CommodityPrices scp;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end