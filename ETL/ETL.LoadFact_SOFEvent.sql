/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the LoadFact_SOFEvent stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	03/15/2019	Added LoadPortBerthKey and DischargePortBerthKey
Brian Boswick	05/20/2019	Remove deleted records from Warehouse
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	02/14/2020	Renamed multiple metrics
Brian Boswick	06/02/2020	Pull event duration from Staging.SOFEvent_Durations table
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	03/25/2021	Refactor to remove Parcel/Product grain and change to event level grain. LImit
							to only pulling in event from last 2 years
Brian Boswick	07/12/2021	Removed COAKey
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
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_SOFEvent', 'U') is not null
		truncate table Staging.Fact_SOFEvent;

	begin try
		insert
				Staging.Fact_SOFEvent with (tablock)
					(
						EventAlternateKey,
						ParcelPortAlternateKey,
						PortKey,
						BerthKey,
						StartDateKey,
						StopDateKey,
						PostFixtureKey,
						VesselKey,
						PortBerthKey,
						ChartererKey,
						OwnerKey,
						ProrationType,
						EventType,
						IsLaytime,
						IsPumpingTime,
						LoadDischarge,
						Comments,
						StartDateTime,
						StopDateTime,
						StartDateTimeSort,
						Duration,
						LaytimeUsed,
						LaytimeAllowed,
						StartTime,
						StopTime,
						StartDate,
						StopDate
					)
		select
			distinct
				sof.QBRecId									EventAlternateKey,
				isnull(parcel.ParcelPortAlternateKey, -1)	ParcelPortAlternateKey,
				isnull([port].PortKey, -1)					PortKey,
				isnull(berth.BerthKey, -1)					BerthKey,
				isnull(startdate.DateKey, 18991230)			StartDateKey,
				isnull(stopdate.DateKey, 47001231)			StopDateKey,
				isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
				isnull(vessel.VesselKey, -1)				VesselKey,
				isnull(portberth.PortBerthKey, -1)			PortBerthKey,
				isnull(wch.ChartererKey, -1)				ChartererKey,
				isnull(wo.OwnerKey, -1)						OwnerKey,
				sof.LaytimeProationType						ProrationType,
				eventtype.EventNameReports					EventType,
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
				parcel.LoadDischarge						LoadDischarge,
				sof.Comments								Comments,
				concat	(
							convert(varchar(50), try_convert(datetime, sof.StartDate), 103),
							' ',
							left(convert(varchar(50), try_convert(time, sof.StartTime), 108), 5)
						)									StartDateTime,
				concat	(
							convert(varchar(50), try_convert(datetime, sof.StopDate), 103),
							' ',
							left(convert(varchar(50), try_convert(time, sof.StopTime), 108), 5)
						)									StopDateTime,
				try_convert	(
								datetime,
								sof.StartDate
								+ ' '
								+ sof.StartTime
							)								StartDateTimeSort,
				ed.EventDuration							Duration,
				sof.LtUsedProrationAmtHrs_QBC				LaytimeUsed,
				parcel.LaytimeAllowed						LaytimeAllowed,
				try_convert(time, sof.StartTime)			StartTime,
				try_convert(time, sof.StopTime)				StopTime,
				try_convert(datetime, sof.StartDate)		StartDate,
				try_convert(datetime, sof.StopDate)			StopDate
			from
				SOFEvents sof with (nolock)
					left join Staging.SOFEvent_Durations ed (nolock)
						on ed.EventAlternateKey = sof.QBRecId
					left join Warehouse.Dim_Calendar startdate with (nolock)
						on try_convert(date, sof.StartDate) = startdate.FullDate
					left join Warehouse.Dim_Calendar stopdate with (nolock)
						on try_convert(date, sof.StopDate) = stopdate.FullDate
					left join PortEventTimes eventtype with (nolock)
						on sof.RelatedPortTimeEventId = eventtype.QBRecId
					join	(
								select
									distinct
										pb.QBRecId						ParcelBerthId,
										pb.RelatedSpiFixtureId			PostFixtureAlternateKey,
										pb.RelatedLDPId					ParcelPortAlternateKey,
										eventport.RelatedPortId			RelatedPortId,
										pb.RelatedBerthId				RelatedBerthId,
										pb.LaytimeAllowedBerthHrs_QBC	LaytimeAllowed,
										loadport.RelatedPortId			LoadPortAlternateKey,
										loadberth.[RelatedBerthId]		LoadBerthAlternateKey,
										loaddischarge.[Type]			LoadDischarge,
										dischport.[RelatedPortId]		DischPortAlternateKey,
										dischberth.RelatedBerthId		DischBerthAlternateKey										
									from
										ParcelBerths pb with (nolock)
											join ParcelPorts eventport with (nolock)
												on eventport.QBRecId = pb.RelatedLDPId
											join Parcels p with (nolock)
												on pb.RelatedSpiFixtureId = p.RelatedSpiFixtureId
											join ParcelPorts loadport with (nolock)
												on loadport.QBRecId = p.RelatedLoadPortID
											join ParcelBerths loadberth with (nolock)
												on loadberth.QBRecId = p.RelatedLoadBerth
											join ParcelPorts dischport with (nolock)
												on dischport.QBRecId = p.RelatedDischPortId
											join ParcelBerths dischberth with (nolock)
												on dischberth.QBRecId = p.RelatedDischBerth
											join ParcelPorts loaddischarge with (nolock)
												on pb.RelatedLDPId = loaddischarge.QBRecId
							) parcel
						on sof.RelatedParcelBerthId = parcel.ParcelBerthId
					left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
						on wpostfixture.PostFixtureAlternateKey = parcel.PostFixtureAlternateKey
					left join PostFixtures epostfixture with (nolock)
						on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
					left join FullStyles fs with (nolock)
						on epostfixture.RelatedChartererFullStyle = fs.QBRecId
					left join Warehouse.Dim_Owner wo with (nolock)
						on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
					left join Warehouse.Dim_Charterer wch with (nolock)
						on wch.ChartererAlternateKey = fs.RelatedChartererParentID
					left join Warehouse.Dim_Vessel vessel with (nolock)
						on vessel.VesselAlternateKey = epostfixture.RelatedVessel
					left join Warehouse.Dim_Port [port] with (nolock)
						on [port].PortAlternateKey = parcel.RelatedPortId
					left join Warehouse.Dim_Berth berth with (nolock)
						on berth.BerthAlternateKey = parcel.RelatedBerthId
					left join Warehouse.Dim_PortBerth portberth with (nolock)
						on portberth.PortAlternateKey = parcel.RelatedPortId
							and portberth.BerthAlternateKey = parcel.RelatedBerthId
			where
				sof.StartDate is not null
				and convert(date, sof.StartDate) >= dateadd(year, -2, getdate());
	end try
	begin catch
		select @ErrorMsg = 'Staging SOFEvent records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update event duration
	begin try
		-- Create full start and stop datetimes
		update
				Staging.Fact_SOFEvent with (tablock)
			set
				StartDate = datetimefromparts(year(StartDate), month(StartDate), day(StartDate), datepart(hour, StartTime), datepart(minute, StartTime), 0, 0),
				StopDate = datetimefromparts(year(StopDate), month(StopDate), day(StopDate), datepart(hour, StopTime), datepart(minute, StopTime), 0, 0);	
	end try
	begin catch
		select @ErrorMsg = 'Updating SOFEvent Duration - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_SOFEvent', 'U') is not null
		truncate table Warehouse.Fact_SOFEvent;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_SOFEvent with (tablock)
					(
						EventAlternateKey,
						PortKey,
						BerthKey,
						StartDateKey,
						StopDateKey,
						PostFixtureKey,
						VesselKey,
						PortBerthKey,
						ChartererKey,
						OwnerKey,
						ProrationType,
						EventType,
						IsLaytime,
						IsPumpingTime,
						LoadDischarge,
						Comments,
						StartDateTime,
						StopDateTime,
						StartDateTimeSort,
						Duration,
						LaytimeUsed,
						LaytimeAllowed,
						RowCreatedDate
					)
			select
					evt.EventAlternateKey,
					evt.PortKey,
					evt.BerthKey,
					evt.StartDateKey,
					evt.StopDateKey,
					evt.PostFixtureKey,
					evt.VesselKey,
					evt.PortBerthKey,
					evt.ChartererKey,
					evt.OwnerKey,
					evt.ProrationType,
					evt.EventType,
					evt.IsLaytime,
					evt.IsPumpingTime,
					evt.LoadDischarge,
					evt.Comments,
					evt.StartDateTime,
					evt.StopDateTime,
					evt.StartDateTimeSort,
					evt.Duration,
					evt.LaytimeUsed,
					evt.LaytimeAllowed,
					getdate() RowCreatedDate
				from
					Staging.Fact_SOFEvent evt with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end