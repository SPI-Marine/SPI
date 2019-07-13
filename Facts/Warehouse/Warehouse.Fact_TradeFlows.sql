/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/12/2019
Description:	Creates the Warehouse.Fact_TradeFlows table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_TradeFlows;
go

create table Warehouse.Fact_TradeFlows
	(
		TradeFlowKey											int					not null identity(1, 1),
		TradeFlowAlternateKey									int					not null,
		ReportingCountryKey										int					not null,
		PartnerCountryKey										int					not null,
		ProductKey												int					not null,
		ReportDateKey											int					not null,
		ImportExport											varchar(50)			null,		-- Degenerate Dimension Attributes
		Unit													varchar(50)			null,
		Quantity												decimal(14, 2)		null,		-- Metrics
		[Value]													decimal(14, 2)		null,
		AveragePrice											decimal(14, 2)		null,		-- Metrics
		RowCreatedDate											date				not null,
		constraint [PK_Warehouse_Fact_TradeFlow_Key] primary key clustered 
		(
			TradeFlowKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];