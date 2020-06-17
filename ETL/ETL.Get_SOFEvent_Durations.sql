/*
==========================================================================================================
Author:			Brian Boswick
Create date:	06/01/2020
Description:	Calculates SOF Event durations
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.Get_SOFEvent_Durations;
go

create procedure ETL.Get_SOFEvent_Durations
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.SOFEvent_Durations', 'U') is not null
		truncate table Staging.SOFEvent_Durations;

	begin try
		insert
				Staging.SOFEvent_Durations with (tablock)	(
																EventAlternateKey,
																PostFixtureAlternateKey,
																ParcelBerthAlternateKey,
																EventTypeId,
																EventType,
																EventStartDateTime,
																EventStopDateTime,
																LoadDischarge,
																IsLaytime,
																IsPumpingTime,
																LaytimeUsedProrated
															)
			select
					sof.QBRecId									EventAlternateKey,
					pb.RelatedSpiFixtureId						PostFixtureAlternateKey,
					sof.RelatedParcelBerthId					ParcelBerthAlternateKey,
					eventtype.QBRecId							EventTypeId,
					eventtype.EventNameReports					EventType,
					datetimefromparts	(
											year(try_convert(date, sof.StartDate)),
											month(try_convert(date, sof.StartDate)),
											day(try_convert(date, sof.StartDate)),
											datepart(hour, try_convert(time, sof.StartTime)),
											datepart(minute, try_convert(time, sof.StartTime)),
											0,
											0
										)						EventStartDateTime,
					datetimefromparts	(
											year(try_convert(date, sof.StopDate)),
											month(try_convert(date, sof.StopDate)),
											day(try_convert(date, sof.StopDate)),
											datepart(hour, try_convert(time, sof.StopTime)),
											datepart(minute, try_convert(time, sof.StopTime)),
											0,
											0
										)						EventStopDateTime,
					loaddischarge.[Type]						LoadDischarge,
					case sof.Laytime
						when 1
							then 'Y'
						else 'N'
					end											IsLaytime,
					case sof.PumpingTime
						when 1
							then 'Y'
						else 'N'
					end											IsPumpingTime,
					sof.LtUsedProrationAmtHrs_QBC				LaytimeUsedProrated
			from
				SOFEvents sof with (nolock)
					left join ParcelBerths pb with (nolock)
						on pb.QBRecId = sof.RelatedParcelBerthId
					left join ParcelPorts loaddischarge
						on pb.RelatedLDPId = loaddischarge.QBRecId
					left join PortEventTimes eventtype with (nolock)
						on sof.RelatedPortTimeEventId = eventtype.QBRecId
			where
				try_convert(date, sof.StartDate) is not null
				and try_convert(date, sof.StartDate) > '1/1/1900'
				and try_convert(time, sof.StartTime) is not null
				and try_convert(date, sof.StopDate) > '1/1/1900'
				and try_convert(time, sof.StopTime) is not null
				and pb.RelatedSpiFixtureId is not null;

	end try
	begin catch
		select @ErrorMsg = 'Staging SOFEvent_Durations records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	--Get NextEventStartDateTime
	begin try
		with NextEvents(EventAlternateKey, PostFixtureAlternateKey, NextStartDateTime)
		as
		(
			select
					ed.EventAlternateKey,
					ed.PostFixtureAlternateKey,
					lead(ed.EventStartDateTime) over	(
															partition by ed.PostFixtureAlternateKey
															order by ed.EventStartDateTime
														)	NextEventStartDateTime
				from
					Staging.SOFEvent_Durations ed
		)

		update
				Staging.SOFEvent_Durations with (tablock)
			set
				NextEventStartDateTime = ne.NextStartDateTime
			from
				NextEvents ne
			where
				ne.EventAlternateKey = Staging.SOFEvent_Durations.EventAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating NextEventStartDateTime - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update event duration
	begin try
		update
				Staging.SOFEvent_Durations with (tablock)
			set
				EventDuration = datediff(minute, EventStartDateTime, EventStopDateTime)/60.0,
				IntraEventDuration = datediff(minute, EventStartDateTime, isnull(NextEventStartDateTime, EventStartDateTime))/60.0;;
	end try
	begin catch
		select @ErrorMsg = 'Updating SOFEvent Duration - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
end