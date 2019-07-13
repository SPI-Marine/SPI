/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/13/2019
Description:	Creates the Warehouse.Fact_CommodityPrices table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_CommodityPrices;
go

create table Warehouse.Fact_CommodityPrices
	(
		CommodityPriceKey										int					not null identity(1, 1),
		CommodityPriceAlternateKey								int					not null,
		ProductKey												int					not null,
		ReportDateKey											int					not null,
		AssessmentType											varchar(50)			null,		-- Degenerate Dimension Attributes
		Unit													varchar(50)			null,
		Remarks													varchar(500)		null,
		PriceHigh												decimal(12, 2)		null,		-- Metrics
		PriceLow												decimal(12, 2)		null,
		PriceAverage											decimal(12, 2)		null,		-- Metrics
		RowCreatedDate											date				not null,
		constraint [PK_Warehouse_Fact_CommodityPrice_Key] primary key clustered 
		(
			CommodityPriceKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];