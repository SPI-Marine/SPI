/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/19/2019
Description:	Creates the Warehouse.Fact_FreightRates table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/12/2020	Added ProductQuantityKey
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_FreightRates;
go

create table Warehouse.Fact_FreightRates
	(
		FreightRateKey											int					not null identity(1, 1),
		FreightRateAlternateKey									int					not null,
		ProductKey												int					not null,
		LoadPortKey												int					not null,
		DischargePortKey										int					not null,
		ReportDateKey											int					not null,
		ProductQuantityKey										int					not null,
		ProductQuantity											decimal(18, 5)		null,		-- Degenerate Dimension Attributes
		Currency												varchar(15)			null,
		FreightRate												decimal(18, 5)		null,		-- Metrics
		RowCreatedDate											date				not null,
		constraint [PK_Warehouse_Fact_FreightRate_Key] primary key clustered 
		(
			FreightRateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];