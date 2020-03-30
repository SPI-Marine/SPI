/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/13/2019
Description:	Creates the Staging.Fact_CommodityPrices table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_CommodityPrices;
go

create table Staging.Fact_CommodityPrices
	(
		CommodityPriceAlternateKey								int					not null,
		ProductKey												int					not null,
		ReportDateKey											int					not null,
		AssessmentType											varchar(50)			null,		-- Degenerate Dimension Attributes
		Unit													varchar(50)			null,
		Remarks													varchar(500)		null,
		PriceHigh												decimal(12, 5)		null,		-- Metrics
		PriceLow												decimal(12, 5)		null,
		PriceAverage											decimal(12, 5)		null,		-- Metrics
		constraint [PK_Staging_Fact_CommodityPrices_AltKey] primary key clustered 
		(
			CommodityPriceAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];