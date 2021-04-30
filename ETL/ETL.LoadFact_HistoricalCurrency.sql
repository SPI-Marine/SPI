set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_HistoricalCurrency;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/30/2021
Description:	Creates the LoadFact_HistoricalCurrency stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_HistoricalCurrency
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_HistoricalCurrency', 'U') is not null
		truncate table Staging.Fact_HistoricalCurrency;

	begin try
		insert
				Staging.Fact_HistoricalCurrency with (tablock)	(
																	HistoricalCurrencyAlternateKey,
																	CurrencyKey,
																	DateKey,
																	UnitsPerUSD,
																	USDPerUnit
																)
		select
				cur.RecordID									HistoricalCurrencyAlternateKey,
				isnull(wc.CurrencyKey, -1)						CurrencyKey,
				isnull(cd.DateKey, -1)							DateKey,
				cur.UnitsPerUSD,
				cur.USDPerUnit
			from
				HistoricalCurrencies cur with (nolock)
					left join Warehouse.Dim_Currency wc with (nolock)
						on wc.CurrencyCode = cur.CurrencyCode
					left join Warehouse.Dim_Calendar cd
						on cd.FullDate = cur.RateDate;
	end try
	begin catch
		select @ErrorMsg = 'Staging Historical Currency records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_HistoricalCurrency', 'U') is not null
		truncate table Warehouse.Fact_HistoricalCurrency;

	-- Insert records into Warehouse table
	begin try
		insert
				Warehouse.Fact_HistoricalCurrency with (tablock)	(
																		HistoricalCurrencyAlternateKey,
																		CurrencyKey,
																		DateKey,
																		UnitsPerUSD,
																		USDPerUnit,
																		RowCreatedDate
																	)
			select
					shc.HistoricalCurrencyAlternateKey,
					shc.CurrencyKey,
					shc.DateKey,
					shc.UnitsPerUSD,
					shc.USDPerUnit,
					getdate()
				from
					Staging.Fact_HistoricalCurrency shc with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end