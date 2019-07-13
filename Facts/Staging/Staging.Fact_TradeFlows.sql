/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/12/2019
Description:	Creates the Staging.Fact_TradeFlows table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_TradeFlows;
go

create table Staging.Fact_TradeFlows
	(
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
		constraint [PK_Staging_Fact_TradeFlows_AltKey] primary key clustered 
		(
			TradeFlowAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];