/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/22/2019
Description:	Creates the Warehouse.Fact_FixtureFinance table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_FixtureFinance;
go

create table Warehouse.Fact_FixtureFinance
	(
		FixtureFinanceKey				int					not null identity(1, 1),
		PostFixtureAlternateKey			int					not null,
		RebillAlternateKey				int					not null,
		ChargeAlternateKey				int					not null,
		ParcelProductAlternateKey		int					not null,
		ProductAlternateKey				int					not null,
		LoadPortKey						int					not null,
		LoadBerthKey					int					not null,
		DischargePortKey				int					not null,
		DischargeBerthKey				int					not null,
		ProductKey						int					not null,
		ParcelKey						int					not null,
		PostFixtureKey					int					not null,
		VesselKey						int					not null,
		ChargeType						nvarchar(500)		null,		-- Degenerate Dimension Attributes
		ChargeDescription				nvarchar(500)		null,
		ParcelNumber					smallint			null,
		Charge							decimal(18, 2)		null,		-- Metrics
		ChargePerMetricTon				decimal(18, 2)		null,
		AddressCommissionRate			decimal(18, 2)		null,
		AddressCommissionAmount			decimal(18, 2)		null,
		AddressCommissionApplied		decimal(18, 2)		null,
		RowCreatedDate					datetime			not null,
		RowUpdatedDate					datetime			not null,
		constraint [PK_Warehouse_Fact_FixtureFinance_QBRecId] primary key clustered 
		(
			FixtureFinanceKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];