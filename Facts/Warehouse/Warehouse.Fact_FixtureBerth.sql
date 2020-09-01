/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/08/2019
Description:	Creates the Warehouse.Fact_FixtureBerth table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	06/07/2019	Added Average Free and Lay times for all fixtures
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey
Brian Boswick	02/10/2020	Added ProductKey
Brian Boswick	02/12/2020	Added ProductQuantityKey
Brian Boswick	02/13/2020	Renamed multiple metrics
Brian Boswick	04/22/2020	Added CPDateKey
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	08/19/2020	Added DischargePortBerthKey, LoadBerthKey
Brian Boswick	08/21/2020	Renamed ProductQuantityKey to ProductFixtureBerthQuantityKey
Brian Boswick	08/28/2020	Added LoadPortBerthKey
==========================================================================================================	
*/

drop table if exists Warehouse.Fact_FixtureBerth;
go

create table Warehouse.Fact_FixtureBerth
	(
		FixtureBerthKey											int					not null identity(1, 1),
		PostFixtureAlternateKey									int					not null,
		PortAlternateKey										int					not null,
		BerthAlternateKey										int					not null,
		LoadDischargeAlternateKey								int					not null,
		ParcelBerthAlternateKey									int					not null,
		LoadPortBerthKey										int					not null,
		DischargePortBerthKey									int					not null,
		PortBerthKey											int					not null,
		PostFixtureKey											int					not null,
		VesselKey												int					not null,
		FirstEventDateKey										int					not null,
		LoadPortKey												int					not null,
		LoadBerthKey											int					not null,
		DischargePortKey										int					not null,
		ChartererKey											int					not null,
		OwnerKey												int					not null,
		ProductKey												int					not null,
		ProductFixtureBerthQuantityKey							int					not null,
		CPDateKey												int					not null,
		COAKey													int					not null,
		LoadDischarge											varchar(50)			null,		-- Degenerate Dimension Attributes
		ProductType												nvarchar(100)		null,
		ParcelQuantityTShirtSize								varchar(50)			null,
		FreeTimeNOR_Berth										decimal(20, 8)		null,		-- Metrics
		AverageFreeTimeNOR_Berth								decimal(20, 8)		null,
		FreeTimeBerth_HoseOn									decimal(20, 8)		null,
		AverageFreeTimeBerth_HoseOn								decimal(20, 8)		null,
		FreeTimeHoseOn_CommenceLoad								decimal(20, 8)		null,
		AverageFreeTimeHoseOn_CommenceLoad						decimal(20, 8)		null,
		FreeTimeHoseOn_CommenceDischarge						decimal(20, 8)		null,
		AverageFreeTimeHoseOn_CommenceDischarge					decimal(20, 8)		null,
		FreeTimeBerth_HoseOff									decimal(20, 8)		null,
		AverageFreeTimeBerth_HoseOff							decimal(20, 8)		null,
		FreeTimeCompleteLoad_HoseOff							decimal(20, 8)		null,
		AverageFreeTimeCompleteLoad_HoseOff						decimal(20, 8)		null,
		FreeTimeCompleteDischarge_HoseOff						decimal(20, 8)		null,
		AverageFreeTimeCompleteDischarge_HoseOff				decimal(20, 8)		null,
		FreeTimeCommenceLoad_CompleteLoad						decimal(20, 8)		null,
		AverageFreeTimeCommenceLoad_CompleteLoad				decimal(20, 8)		null,
		FreeTimeCommenceDischarge_CompleteDischarge				decimal(20, 8)		null,
		AverageFreeTimeCommenceDischarge_CompleteDischarge		decimal(20, 8)		null,
		DurationNOR_Berth										decimal(20, 8)		null,
		AverageDurationNOR_Berth								decimal(20, 8)		null,
		DurationBerth_HoseOn									decimal(20, 8)		null,
		AverageDurationBerth_HoseOn								decimal(20, 8)		null,
		DurationHoseOn_CommenceLoad								decimal(20, 8)		null,
		AverageDurationHoseOn_CommenceLoad						decimal(20, 8)		null,
		DurationHoseOn_CommenceDischarge						decimal(20, 8)		null,
		AverageDurationHoseOn_CommenceDischarge					decimal(20, 8)		null,
		DurationBerth_HoseOff									decimal(20, 8)		null,
		AverageDurationBerth_HoseOff							decimal(20, 8)		null,
		DurationCompleteLoad_HoseOff							decimal(20, 8)		null,
		AverageDurationCompleteLoad_HoseOff						decimal(20, 8)		null,
		DurationCompleteDischarge_HoseOff						decimal(20, 8)		null,
		AverageDurationCompleteDischarge_HoseOff				decimal(20, 8)		null,
		DurationCommenceLoad_CompleteLoad						decimal(20, 8)		null,
		AverageDurationCommenceLoad_CompleteLoad				decimal(20, 8)		null,
		DurationCommenceDischarge_CompleteDischarge				decimal(20, 8)		null,
		AverageDurationCommenceDischarge_CompleteDischarge		decimal(20, 8)		null,
		TimeToCountNOR_Berth									decimal(20, 8)		null,
		AverageTimeToCountNOR_Berth								decimal(20, 8)		null,
		TimeToCountBerth_HoseOn									decimal(20, 8)		null,
		AverageTimeToCountBerth_HoseOn							decimal(20, 8)		null,
		TimeToCountHoseOn_CommenceLoad							decimal(20, 8)		null,
		AverageTimeToCountHoseOn_CommenceLoad					decimal(20, 8)		null,
		TimeToCountHoseOn_CommenceDischarge						decimal(20, 8)		null,
		AverageTimeToCountHoseOn_CommenceDischarge				decimal(20, 8)		null,
		TimeToCountBerth_HoseOff								decimal(20, 8)		null,
		AverageTimeToCountBerth_HoseOff							decimal(20, 8)		null,
		TimeToCountCompleteLoad_HoseOff							decimal(20, 8)		null,
		AverageTimeToCountCompleteLoad_HoseOff					decimal(20, 8)		null,
		TimeToCountCompleteDischarge_HoseOff					decimal(20, 8)		null,
		AverageTimeToCountCompleteDischarge_HoseOff				decimal(20, 8)		null,
		TimeToCountCommenceLoad_CompleteLoad					decimal(20, 8)		null,
		AverageTimeToCountCommenceLoad_CompleteLoad				decimal(20, 8)		null,
		TimeToCountCommenceDischarge_CompleteDischarge			decimal(20, 8)		null,
		AverageTimeToCountCommenceDischarge_CompleteDischarge	decimal(20, 8)		null,
		TimeToCountPumpingTime									decimal(20, 8)		null,
		AverageLayTimePumpingTime								decimal(20, 8)		null,
		TimeToCountPumpingRate									decimal(20, 8)		null,
		AverageLayTimePumpingRate								decimal(20, 8)		null,
		ParcelQuantity											decimal(20, 8)		null,
		LaytimeUsed												decimal(20, 8)		null,
		AverageLaytimeUsed										decimal(20, 8)		null,
		LaytimeAllowed											decimal(20, 8)		null,
		AverageLaytimeAllowed									decimal(20, 8)		null,
		PumpTime												decimal(20, 8)		null,
		AveragePumpTime											decimal(20, 8)		null,
		WithinLaycanOriginal									smallint			null,
		LaycanOverUnderOriginal									decimal(18,6)		null,
		WithinLaycanFinal										smallint			null,
		LaycanOverUnderFinal									decimal(18,6)		null,
		WithinLaycanNarrowed									smallint			null,
		LaycanOverUnderNarrowed									decimal(18,6)		null,
		VoyageDuration											int					null,		-- 1st NOR to last NOR on Post Fixture
		TransitTime												decimal(20, 8)		null,
		FirstFixtureBerth										tinyint				null,
		FirstPortBerth											tinyint				null,
		WaitingTimeCandidate									tinyint				null,
		RowCreatedDate											date				not null,
		RowUpdatedDate											date				not null,
		constraint [PK_Warehouse_Fact_FixtureBerth_Key] primary key clustered 
		(
			FixtureBerthKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];