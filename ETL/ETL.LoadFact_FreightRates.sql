set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_FreightRates;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/19/2019
Description:	Creates the LoadFact_FreightRates stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/12/2020	Added ProductQuantityKey ETL logic
==========================================================================================================	
*/

create procedure ETL.LoadFact_FreightRates
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_FreightRates', 'U') is not null
		truncate table Staging.Fact_FreightRates;

	begin try
		insert
				Staging.Fact_FreightRates with (tablock)
		select
			distinct
				fr.QBRecId									FreightRateAlternateKey,
				isnull(product.ProductKey, -1)				ProductKey,
				isnull(lp.PortKey, -1)						LoadPortKey,
				isnull(dp.PortKey, -1)						DischargePortKey,
				isnull(rd.DateKey, 18991230)				ReportDateKey,
				isnull(pq.ProductQuantityKey, -1)			ProductQuantityKey,
				fr.ProductQty								ProductQuantity,
				fr.Currency									Currency,
				fr.FrtRate									FreightRate
			from
				FreightRates fr with (nolock)
					left join Warehouse.Dim_Product product with (nolock)
						on product.ProductAlternateKey = fr.RelatedProductId
					left join Warehouse.Dim_Port lp with (nolock)
						on lp.PortAlternateKey = fr.RelatedLoadPortId
					left join Warehouse.Dim_Port dp with (nolock)
						on dp.PortAlternateKey = fr.DischRelatedPortId
					left join Warehouse.Dim_Calendar rd with (nolock)
						on rd.FullDate = convert(date, fr.DateReported)
					left join Warehouse.Dim_ProductQuantity pq with (nolock)
							on convert(decimal(18, 4), fr.ProductQty) between pq.MinimumQuantity and pq.MaximumQuantity;
	end try
	begin catch
		select @ErrorMsg = 'Staging FreightRates records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_FreightRates', 'U') is not null
		truncate table Warehouse.Fact_FreightRates;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_FreightRates with (tablock)
			select
					sfr.FreightRateAlternateKey,
					sfr.ProductKey,
					sfr.LoadPortKey,
					sfr.DischargePortKey,
					sfr.ReportDateKey,
					sfr.ProductQuantityKey,
					sfr.ProductQuantity,
					sfr.Currency,
					sfr.FreightRate,
					getdate() RowStartDate
				from
					Staging.Fact_FreightRates sfr with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end