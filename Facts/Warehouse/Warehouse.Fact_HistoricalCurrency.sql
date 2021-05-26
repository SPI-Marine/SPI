drop table if exists Warehouse.Fact_HistoricalCurrency;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/30/2021
Description:	Creates the Staging.Fact_HistoricalCurrency table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create table Warehouse.Fact_HistoricalCurrency
	(
		HistoricalCurrencyKey					int					not null identity(1, 1),
		HistoricalCurrencyAlternateKey			int					not null,
		CurrencyKey								int					not null,
		DateKey									int					not null,
		UnitsPerUSD								numeric(30, 9)		null,		-- Metrics
		USDPerUnit								numeric(30, 9)		null,
		RowCreatedDate							datetime			not null,
		constraint [PK_Warehouse_Fact_HistoricalCurrency_Key] primary key clustered 
		(
			HistoricalCurrencyKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];