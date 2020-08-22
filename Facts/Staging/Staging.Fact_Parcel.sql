/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/16/2019
Description:	Creates the Staging.Fact_Parcel table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	01/02/2020	Added LoadNORStartDate, DischargeNORStartDate
Brian Boswick	01/09/2020	Added CPDateKey
Brian Boswick	02/05/2020	Added ChartererKey and OwnerKey
Brian Boswick	02/11/2020	Added VesselKey
Brian Boswick	02/14/2020	Added ProductQuantityKey
Brian Boswick	02/20/2020	Added BunkerCharge
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	07/30/2020	Added NOR/Hose Off dates for load/discharge ports
Brian Boswick	08/21/2020	Changed ProductQuantityKey logic to aggregate to Fixture level
==========================================================================================================	
*/

drop table if exists Staging.Fact_Parcel;
go

create table Staging.Fact_Parcel
	(
		ParcelAlternateKey						int					not null,
		PostFixtureKey							int					not null,
		LoadPortKey								int					not null,
		DischargePortKey						int					not null,
		LoadBerthKey							int					not null,
		DischargeBerthKey						int					not null,
		LoadPortBerthKey						int					not null,
		DischargePortBerthKey					int					not null,
		ProductKey								int					not null,
		BillLadingDateKey						int					not null,
		DimParcelKey							int					not null,
		CPDateKey								int					not null,
		ChartererKey							int					not null,
		OwnerKey								int					not null,
		VesselKey								int					not null,
		ProductPortQuantityKey					int					not null,
		COAKey									int					not null,
		OutTurnQty								decimal(18, 6)		null,			-- Metrics
		ShipLoadedQty							decimal(18, 6)		null,
		ShipDischargeQty						decimal(18, 6)		null,
		NominatedQty							decimal(18, 6)		null,
		BLQty									decimal(18, 6)		null,
		ParcelFreightAmountQBC					decimal(18, 6)		null,
		DemurrageVaultEstimateAmount_QBC		decimal(18, 6)		null,
		DemurrageAgreedAmount_QBC				decimal(18, 6)		null,
		DemurrageClaimAmount_QBC				decimal(18, 6)		null,
		DeadfreightQty							decimal(18, 6)		null,
		LoadLaytimeAllowed						decimal(18, 6)		null,
		LoadLaytimeUsed							decimal(18, 6)		null,
		DischargeLaytimeAllowed					decimal(18, 6)		null,
		DischargeLaytimeUsed					decimal(18, 6)		null,
		BunkerCharge							decimal(18, 6)		null,
		LoadNORStartDate						date				null,
		LoadLastHoseOffDate						date				null,
		DischargeNORStartDate					date				null,
		DischargeLastHoseOffDate				date				null,
		TotalLoadBerthBLQty						decimal(18, 6)		null,			-- ETL fields
		TotalDischargeBerthBLQty				decimal(18, 6)		null,
		LoadPortAlternateKey					int					null,
		DischargePortAlternateKey				int					null,
		PostFixtureAlternateKey					int					null,
		constraint [PK_Staging_Fact_Parcel_AltKey] primary key clustered 
		(
			ParcelAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];