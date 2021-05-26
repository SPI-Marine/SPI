drop table if exists Staging.Fact_HistoricalCurrency;
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

create table Staging.Fact_HistoricalCurrency
	(
		HistoricalCurrencyAlternateKey			int					not null,
		CurrencyKey								int					not null,
		DateKey									int					not null,
		UnitsPerUSD								numeric(30, 9)		null,		-- Metrics
		USDPerUnit								numeric(30, 9)		null,
		constraint [PK_Staging_Fact_HistoricalCurrency_AltKey] primary key clustered 
		(
			HistoricalCurrencyAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];