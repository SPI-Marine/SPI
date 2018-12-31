/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the LoadFact_SOFEvent stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_SOFEvent;
go

create procedure ETL.LoadFact_SOFEvent
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_SOFEvent', 'U') is not null
		truncate table Staging.Fact_SOFEvent;

	begin try
		insert
				Staging.Fact_SOFEvent
		select
				sof.QBRecId	EventAlternateKey,
				parcel.ParcelId	ParcelAlternateKey,
				isnull(parcel.ParcelPortAlternateKey, -1),
				-1	PortKey,
				-1	BerthKey,
				isnull(startdate.DateKey, 18991230) StartDateKey,
				isnull(stopdate.DateKey, 47001231) StopDateKey,
				-1	ProductKey,
				isnull(wpostfixture.PostFixtureKey, -1),
				isnull(vessel.VesselKey, -1),
				sof.ProationType,
				eventtype.EventNameReports	EventType,
				case sof.Laytime
					when 1
						then 'Y'
					else 'N'
				end	IsLaytime,
				case sof.PumpingTime
					when 1
						then 'Y'
					else 'N'
				end	IsPumpingTime,
				null	LoadDischarge,
				sof.Comments,
				null	Duration,
				null	LaytimeActual,
				null	LaytimeAllowed,
				try_convert(time, sof.StartTime),
				try_convert(time, sof.StopTime),
				try_convert(datetime, sof.StartDate),
				try_convert(datetime, sof.StopDate),
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				SOFEvents sof
					left join Warehouse.Dim_Calendar startdate
						on try_convert(date, sof.StartDate) = startdate.FullDate
					left join Warehouse.Dim_Calendar stopdate
						on try_convert(date, sof.StopDate) = stopdate.FullDate
					left join PortEventTimes eventtype
						on sof.RelatedPortTimeEventId = eventtype.QBRecId
					join	(
								select
									distinct
										pb.QBRecId ParcelBerthId,
										p.QbRecId ParcelId,
										pb.RelatedSpiFixtureId PostFixtureAlternateKey,
										pb.RelatedLDPId ParcelPortAlternateKey
									from
										ParcelBerths pb
											join Parcels p
												on pb.RelatedSpiFixtureId = p.RelatedSpiFixtureId
							) parcel
						on sof.RelatedParcelBerthId = parcel.ParcelBerthId
					left join Warehouse.Dim_PostFixture wpostfixture
						on wpostfixture.PostFixtureAlternateKey = parcel.PostFixtureAlternateKey
					left join PostFixtures epostfixture
						on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Vessel vessel
						on vessel.VesselAlternateKey = epostfixture.RelatedVessel
					left join	(
									select
											@ExistingRecord RecordStatus,
											EventAlternateKey
										from
											Warehouse.Fact_SOFEvent
								) rs
						on rs.EventAlternateKey = sof.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging SOFEvent records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update event duration
	begin try
		-- Create full start and stop datetimes
		update
				Staging.Fact_SOFEvent
			set
				StartDate = datetimefromparts(year(StartDate), month(StartDate), day(StartDate), datepart(hour, StartTime), datepart(minute, StartTime), 0, 0),
				StopDate = datetimefromparts(year(StopDate), month(StopDate), day(StopDate), datepart(hour, StopTime), datepart(minute, StopTime), 0, 0);
	
		-- Calculate Duration
		update
				Staging.Fact_SOFEvent
			set
				Duration =	case
								when StartDateKey > 19000000 and StopDateKey < 47000000
									then datediff(minute, StartDate, StopDate)/60.0
								else null
							end,
				LaytimeActual =	case
									when StartDateKey > 19000000 and StopDateKey < 47000000
										then
											case IsLaytime
												when 'Y'
													then datediff(minute, StartDate, StopDate)/60.0
												else null
											end
									else null
								end;
	end try
	begin catch
		select @ErrorMsg = 'Updating SOFEvent duration - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Insert new events into Warehouse table
	begin try
		print 1;
		--insert
		--		Warehouse.Fact_SOFEvent
		--	select
		--			berth.BerthAlternateKey,
		--			berth.BerthName,
		--			berth.DraftRestriction,
		--			berth.LOARestriction,
		--			berth.ProductRestriction,
		--			berth.ExNames,
		--			berth.UniqueId,
		--			berth.UpRiverPorts,
		--			berth.Type1HashValue,
		--			getdate() RowStartDate,
		--			getdate() RowUpdatedDate,
		--			'Y' IsCurrentRow
		--		from
		--			Staging.Fact_SOFEvent berth
		--		where
		--			berth.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end