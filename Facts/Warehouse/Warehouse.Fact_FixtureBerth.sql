/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/08/2019
Description:	Creates the Warehouse.Fact_FixtureBerth table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	06/07/2019	Added Average Wait and Lay times for all fixtures
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
		PortBerthKey											int					not null,
		PostFixtureKey											int					not null,
		VesselKey												int					not null,
		FirstEventDateKey										int					not null,
		LoadDischarge											varchar(50)			null,		-- Degenerate Dimension Attributes
		ProductType												nvarchar(100)		null,
		ParcelQuantityTShirtSize								varchar(50)			null,
		WaitTimeNOR_Berth										decimal(20, 8)		null,		-- Metrics
		AverageWaitTimeNOR_Berth								decimal(20, 8)		null,
		WaitTimeBerth_HoseOn									decimal(20, 8)		null,
		AverageWaitTimeBerth_HoseOn								decimal(20, 8)		null,
		WaitTimeHoseOn_CommenceLoad								decimal(20, 8)		null,
		AverageWaitTimeHoseOn_CommenceLoad						decimal(20, 8)		null,
		WaitTimeHoseOn_CommenceDischarge						decimal(20, 8)		null,
		AverageWaitTimeHoseOn_CommenceDischarge					decimal(20, 8)		null,
		WaitTimeBerth_HoseOff									decimal(20, 8)		null,
		AverageWaitTimeBerth_HoseOff							decimal(20, 8)		null,
		WaitTimeCompleteLoad_HoseOff							decimal(20, 8)		null,
		AverageWaitTimeCompleteLoad_HoseOff						decimal(20, 8)		null,
		WaitTimeCompleteDischarge_HoseOff						decimal(20, 8)		null,
		AverageWaitTimeCompleteDischarge_HoseOff				decimal(20, 8)		null,
		WaitTimeCommenceLoad_CompleteLoad						decimal(20, 8)		null,
		AverageWaitTimeCommenceLoad_CompleteLoad				decimal(20, 8)		null,
		WaitTimeCommenceDischarge_CompleteDischarge				decimal(20, 8)		null,
		AverageWaitTimeCommenceDischarge_CompleteDischarge		decimal(20, 8)		null,
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
		LayTimeNOR_Berth										decimal(20, 8)		null,
		AverageLayTimeNOR_Berth									decimal(20, 8)		null,
		LayTimeBerth_HoseOn										decimal(20, 8)		null,
		AverageLayTimeBerth_HoseOn								decimal(20, 8)		null,
		LayTimeHoseOn_CommenceLoad								decimal(20, 8)		null,
		AverageLayTimeHoseOn_CommenceLoad						decimal(20, 8)		null,
		LayTimeHoseOn_CommenceDischarge							decimal(20, 8)		null,
		AverageLayTimeHoseOn_CommenceDischarge					decimal(20, 8)		null,
		LayTimeBerth_HoseOff									decimal(20, 8)		null,
		AverageLayTimeBerth_HoseOff								decimal(20, 8)		null,
		LayTimeCompleteLoad_HoseOff								decimal(20, 8)		null,
		AverageLayTimeCompleteLoad_HoseOff						decimal(20, 8)		null,
		LayTimeCompleteDischarge_HoseOff						decimal(20, 8)		null,
		AverageLayTimeCompleteDischarge_HoseOff					decimal(20, 8)		null,
		LaytimeCommenceLoad_CompleteLoad						decimal(20, 8)		null,
		AverageLaytimeCommenceLoad_CompleteLoad					decimal(20, 8)		null,
		LaytimeCommenceDischarge_CompleteDischarge				decimal(20, 8)		null,
		AverageLaytimeCommenceDischarge_CompleteDischarge		decimal(20, 8)		null,
		LayTimePumpingTime										decimal(20, 8)		null,
		AverageLayTimePumpingTime								decimal(20, 8)		null,
		LayTimePumpingRate										decimal(20, 8)		null,
		AverageLayTimePumpingRate								decimal(20, 8)		null,
		ParcelQuantity											decimal(20, 8)		null,
		LaytimeActual											decimal(20, 8)		null,
		AverageLaytimeActual									decimal(20, 8)		null,
		LaytimeAllowed											decimal(20, 8)		null,
		AverageLaytimeAllowed									decimal(20, 8)		null,
		PumpTime												decimal(20, 8)		null,
		AveragePumpTime											decimal(20, 8)		null,
		WithinLaycanOriginal									smallint			null,
		LaycanOverUnderOriginal									int					null,
		WithinLaycanFinal										smallint			null,
		LaycanOverUnderFinal									int					null,
		VoyageDuration											int					null,		-- 1st NOR to last NOR on Post Fixture
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