/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/19/2019
Description:	Creates the Staging.Fact_FreightRates table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Fact_FreightRates;
go

create table Staging.Fact_FreightRates
	(
		FreightRateAlternateKey									int					not null,
		ProductKey												int					not null,
		LoadPortKey												int					not null,
		DischargePortKey										int					not null,
		ReportDateKey											int					not null,
		ProductQuantity											decimal(18, 2)		null,		-- Degenerate Dimension Attributes
		Currency												varchar(15)			null,
		FreightRate												decimal(18, 2)		null,		-- Metrics
		constraint [PK_Staging_Fact_FreightRates_AltKey] primary key clustered 
		(
			FreightRateAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];