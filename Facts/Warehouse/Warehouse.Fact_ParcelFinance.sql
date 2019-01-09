/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/08/2019
Description:	Creates the Warehouse.Fact_ParcelFinance table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_ParcelFinance;
go

create table Warehouse.Fact_ParcelFinance
	(
		ParcelFinanceKey		int					not null identity(1, 1),
		PostFixtureAlternateKey	int					not null,
		ParcelAlternateKey		int					not null,
		PortKey					int					not null,
		BerthKey				int					not null,
		ProductKey				int					not null,
		PostFixtureKey			int					not null,
		VesselKey				int					not null,
		ChargeType				nvarchar(500)		null,		-- Degenerate Dimension Attributes
		ChargeDescription		nvarchar(500)		null,
		ParcelNumber			smallint			null,
		Charge					decimal(18, 4)		null,		-- Metrics
		RecordStatus			int					not null,
		constraint [PK_Warehouse_Fact_ParcelFinance_QBRecId] primary key clustered 
		(
			ParcelFinanceKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];