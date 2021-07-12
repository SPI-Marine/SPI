drop table if exists Warehouse.Fact_FixtureFinance;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/22/2019
Description:	Creates the Warehouse.Fact_FixtureFinance table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	04/13/2019	Added LoadPortBerthKey and DischargePortBerthKey
Brian Boswick	01/31/2020	Added LoadPortKey and DischargePortKey
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	06/18/2021	Added ProductFixtureQuantityKey
Brian Boswick	07/12/2021	Removed COAKey
==========================================================================================================	
*/

create table Warehouse.Fact_FixtureFinance
	(
		FixtureFinanceKey				int					not null identity(1, 1),
		PostFixtureAlternateKey			int					not null,
		RebillAlternateKey				int					not null,
		ChargeAlternateKey				int					not null,
		ParcelProductAlternateKey		int					not null,
		ProductAlternateKey				int					not null,
		ParcelAlternateKey				int					not null,
		ChargeTypeAlternateKey			smallint			not null,
		LoadPortBerthKey				int					not null,
		DischargePortBerthKey			int					not null,
		LoadPortKey						int					not null,
		DischargePortKey				int					not null,
		ProductKey						int					not null,
		ParcelKey						int					not null,
		PostFixtureKey					int					not null,
		VesselKey						int					not null,
		CharterPartyDateKey				int					not null,
		FirstLoadEventDateKey			int					not null,
		ChartererKey					int					not null,
		OwnerKey						int					not null,
		ProductFixtureQuantityKey		int					not null,
		ChargeType						nvarchar(500)		null,		-- Degenerate Dimension Attributes
		ChargeDescription				nvarchar(500)		null,
		ParcelNumber					smallint			null,
		Charge							decimal(20, 8)		null,		-- Metrics
		ChargePerMetricTon				decimal(20, 8)		null,
		AddressCommissionRate			decimal(20, 8)		null,
		AddressCommissionAmount			decimal(20, 8)		null,
		AddressCommissionApplied		decimal(20, 8)		null,
		RowCreatedDate					datetime			not null,
		constraint [PK_Warehouse_Fact_FixtureFinance_QBRecId] primary key clustered 
		(
			FixtureFinanceKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];