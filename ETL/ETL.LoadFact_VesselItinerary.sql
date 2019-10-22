set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_VesselItinerary;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/13/2019
Description:	Creates the LoadFact_VesselItinerary stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	10/20/2019	Added Original ETA benchmark metrics
==========================================================================================================	
*/

create procedure ETL.LoadFact_VesselItinerary
as
begin
	set nocount on;

	declare	@ErrorMsg			varchar(1000),
			@NewRecord			int = 1,
			@ExistingRecord		int = 2;

	-- Clear Staging table
	if object_id(N'Staging.Fact_VesselItinerary', 'U') is not null
		truncate table Staging.Fact_VesselItinerary;

	begin try
		insert
				Staging.Fact_VesselItinerary with (tablock)	(
																VesselItineraryAlternateKey,
																PostFixtureKey,
																PortKey,
																ETAStartDateKey,
																ETAEndDateKey,
																DateModifiedKey,
																ItineraryPortType,
																Comments,
																NORStartDate,
																ETAOriginalDate,
																ETAOriginalCreateDate,
																MostRecentETADate,
																LoadDischarge,
																RelatedParcelPortID,
																RelatedPortID,
																ETAChanged,
																DateModified,
																RecordStatus
															)
		select
			distinct
				vi.RecordID									VesselItineraryAlternateKey,
				isnull(fixture.PostFixtureKey, -1)			PostFixtureKey,
				-1											PortKey,
				isnull(sd.DateKey, 18991230)				ETAStartDateKey,
				coalesce(ed.DateKey, sd.DateKey, 18991230)	ETAEndDateKey,
				isnull(dm.DateKey, 47001231)				DateModifiedKey,	--- REMOVE THIS FIELD AFTER LETTING RACHEL KNOW ---
				vi.ItineraryPortType						ItineraryPortType,
				vi.Comments									Comments,
				firstnorevent.FirstNOREventDate				NORStartDate,
				vi.ETAStartOriginal_ADMIN					ETAOriginalDate,
				vi.OriginalETACreatedOn_ADMIN				ETAOriginalCreateDate,
				vi.ETAStart									MostRecentETADate,
				loaddischarge.[Type]						LoadDischarge,
				vi.RelatedParcelPortID						RelatedParcelPortID,
				vi.RelatedPortID							RelatedPortID,
				case
					when convert(date, isnull(vi.ETAStart, '12/30/1899')) <> isnull(wvi.MostRecentETADate, '12/30/1899')
						then 1
					else 0
				end											ETAChanged,
				vi.DateModified								DateModified,
				isnull(rs.RecordStatus, @NewRecord)			RecordStatus
			from
				VesselItinerary vi with (nolock)
					left join Warehouse.Dim_PostFixture fixture with (nolock)
						on vi.RelatedSPIFixtureID = fixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar sd with (nolock)
						on sd.FullDate = try_convert(date, vi.ETAStart)
					left join Warehouse.Dim_Calendar ed with (nolock)
						on ed.FullDate = try_convert(date, vi.ETAEnd)
					left join Warehouse.Dim_Calendar dm with (nolock)
						on dm.FullDate = try_convert(date, vi.DateModified)
					left join ParcelPorts loaddischarge with (nolock)
						on loaddischarge.QBRecId = vi.RelatedParcelPortID
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											min(e.StartDate)	FirstNOREventDate
										from
											SOFEvents e with (nolock)
												join PortEventTimes pet with (nolock)
													on pet.QBRecId = e.RelatedPortTimeEventId
												join ParcelBerths pb with (nolock)
													on pb.QBRecId = e.RelatedParcelBerthId
												join ParcelPorts pp with (nolock)
													on pp.QBRecId = pb.RelatedLDPId
												join PostFixtures pf with (nolock)
													on pf.QBRecId = pb.RelatedSpiFixtureId
												where
													pet.EventNameReports like 'NOR Tend%'
												group by
													pf.QBRecId, pp.RelatedPortId
								) firstnorevent
						on firstnorevent.PostFixtureAlternateKey = vi.RelatedSPIFixtureID
							and firstnorevent.RelatedPortID = loaddischarge.RelatedPortId
					left join Warehouse.Fact_VesselItinerary wvi with (nolock)
						on wvi.VesselItineraryAlternateKey = vi.RecordID
					left join	(
									select
											@ExistingRecord RecordStatus,
											VesselItineraryAlternateKey
										from
											Warehouse.Fact_VesselItinerary with (nolock)
								) rs
						on rs.VesselItineraryAlternateKey = vi.RecordID;
	end try
	begin catch
		select @ErrorMsg = 'Staging VesselItinerary records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ETALastModifiedDate/MostRecentETADate changed
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				ETALastModifiedDate =	case
											when ETAChanged = 1
												then vi.DateModified
											else wvi.ETALastModifiedDate
										end,
				MostRecentETADate =	case
										when ETAChanged = 1
											then vi.MostRecentETADate
										else wvi.MostRecentETADate
									end
			from
				Staging.Fact_VesselItinerary vi
					left join Warehouse.Fact_VesselItinerary wvi with (nolock)
						on wvi.VesselItineraryAlternateKey = vi.VesselItineraryAlternateKey;

	end try
	begin catch
		select @ErrorMsg = 'Updating MostRecentETADate, ETALastModifiedDate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update DaysBetweenRecentETALastModified
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				DaysBetweenRecentETALastModified = abs(datediff(day, vi.MostRecentETADate, vi.ETALastModifiedDate))
			from
				Staging.Fact_VesselItinerary vi;

	end try
	begin catch
		select @ErrorMsg = 'Updating MostRecentETADate, ETALastModifiedDate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update One and Two Week ETAs
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				OneWeekETA =	case
									when isnull(OneWeekETA, '12/30/1899') = '12/30/1899'
											and DaysBetweenRecentETALastModified between 6 and 9
										then vi.MostRecentETADate
									else OneWeekETA
								end,
				TwoWeekETA =	case
									when isnull(TwoWeekETA, '12/30/1899') = '12/30/1899'
											and DaysBetweenRecentETALastModified between 13 and 16
										then vi.MostRecentETADate
									else TwoWeekETA
								end
			from
				Staging.Fact_VesselItinerary vi;

	end try
	begin catch
		select @ErrorMsg = 'Updating MostRecentETADate, ETALastModifiedDate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update DaysOutOriginalETASent
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				DaysOutOriginalETASent =	datediff(day, NORStartDate, ETAOriginalCreateDate)
			from
				Staging.Fact_VesselItinerary vi
			where
				isnull(vi.NORStartDate, '12/30/1899') > '12/30/1899';

	end try
	begin catch
		select @ErrorMsg = 'Updating MostRecentETADate, ETALastModifiedDate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Insert NOR Tendered records
	begin try
		insert
				Staging.Fact_VesselItinerary with (tablock)	(
																VesselItineraryAlternateKey,
																PostFixtureKey,
																PortKey,
																ETAStartDateKey,
																ETAEndDateKey,
																DateModifiedKey,
																ItineraryPortType,
																Comments,
																RelatedParcelPortID,
																RelatedPortID
															)
		select
			distinct
				(e.QBRecId * -1)							VesselItineraryAlternateKey,
				isnull(fixture.PostFixtureKey, -1)			PostFixtureKey,
				-1											PortKey,
				isnull(sd.DateKey, 18991230)				ETAStartDateKey,
				isnull(sd.DateKey, 18991230)				ETAEndDateKey,
				isnull(dm.DateKey, 47001231)				DateModifiedKey,
				'NOR Tendered'								ItineraryPortType,
				null										Comments,
				null										RelatedParcelPortID,
				eventport.RelatedPortId						RelatedPortID
			from
				SOFEvents e with (nolock)
					left join PortEventTimes eventtype with (nolock)
						on eventtype.QBRecId = e.RelatedPortTimeEventId
					left join ParcelBerths pb with (nolock)
						on e.RelatedParcelBerthId = pb.QBRecId
					join ParcelPorts eventport with (nolock)
						on eventport.QBRecId = pb.RelatedLDPId
					left join Warehouse.Dim_PostFixture fixture with (nolock)
						on pb.RelatedSpiFixtureId = fixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar sd with (nolock)
						on sd.FullDate = try_convert(date, e.StartDate)
					left join Warehouse.Dim_Calendar dm
						on dm.FullDate = try_convert(date, e.DateModified)
			where
				eventtype.EventNameReports like 'NOR Tendered';
	end try
	begin catch
		select @ErrorMsg = 'Inserting NOR Tendered records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update PortKey
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				PortKey =	case
								when vi.ItineraryPortType like 'Fixture%'
									then isnull(fpt.PortKey, -1)
								when vi.ItineraryPortType like 'Itinerary%' or vi.ItineraryPortType like 'NOR%'
									then isnull(ipt.PortKey, -1)
								else -1
							end
			from
				Staging.Fact_VesselItinerary vi
					left join Warehouse.Dim_Port ipt with (nolock)
						on ipt.PortAlternateKey = vi.RelatedPortID
					left join ParcelPorts pp with (nolock)
						on pp.QBRecId = vi.RelatedParcelPortID
					left join Warehouse.Dim_Port fpt with (nolock)
						on fpt.PortAlternateKey = pp.RelatedPortId
			where
				vi.VesselItineraryAlternateKey = VesselItineraryAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging VesselItinerary records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_VesselItinerary with (tablock)	(
																	VesselItineraryAlternateKey,
																	PostFixtureKey,
																	PortKey,
																	ETAStartDateKey,
																	ETAEndDateKey,
																	DateModifiedKey,
																	ItineraryPortType,
																	Comments,
																	NORStartDate,
																	ETAOriginalDate,
																	ETAOriginalCreateDate,
																	TwoWeekETA,
																	OneWeekETA,
																	MostRecentETADate,
																	ETALastModifiedDate,
																	LoadDischarge,
																	DaysOutOriginalETASent,
																	DaysBetweenRecentETALastModified,
																	RowCreatedDate,
																	RowUpdatedDate
																)
			select
					fvi.VesselItineraryAlternateKey,
					fvi.PostFixtureKey,
					fvi.PortKey,
					fvi.ETAStartDateKey,
					fvi.ETAEndDateKey,
					fvi.DateModifiedKey,
					fvi.ItineraryPortType,
					fvi.Comments,
					fvi.NORStartDate,
					fvi.ETAOriginalDate,
					fvi.ETAOriginalCreateDate,
					fvi.TwoWeekETA,
					fvi.OneWeekETA,
					fvi.MostRecentETADate,
					fvi.ETALastModifiedDate,
					fvi.LoadDischarge,
					fvi.DaysOutOriginalETASent,
					fvi.DaysBetweenRecentETALastModified,
					getdate() RowCreatedDate,
					getdate() RowUpdatedDate
				from
					Staging.Fact_VesselItinerary fvi with (nolock)
				where
					fvi.RecordStatus = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing Warehouse records
	begin try
		update
				Warehouse.Fact_VesselItinerary with (tablock)
			set
				PostFixtureKey = fvi.PostFixtureKey,
				PortKey = fvi.PortKey,
				ETAStartDateKey =  fvi.ETAStartDateKey,
				ETAEndDateKey = fvi.ETAEndDateKey,
				DateModifiedKey = fvi.DateModifiedKey,
				ItineraryPortType = fvi.ItineraryPortType,
				Comments = fvi.Comments,
				NORStartDate = fvi.NORStartDate,
				ETAOriginalDate = fvi.ETAOriginalDate,
				ETAOriginalCreateDate = fvi.ETAOriginalCreateDate,
				TwoWeekETA = fvi.TwoWeekETA,
				OneWeekETA = fvi.OneWeekETA,
				MostRecentETADate = fvi.MostRecentETADate,
				ETALastModifiedDate = fvi.ETALastModifiedDate,
				LoadDischarge = fvi.LoadDischarge,
				DaysOutOriginalETASent = fvi.DaysOutOriginalETASent,
				DaysBetweenRecentETALastModified = fvi.DaysBetweenRecentETALastModified,
				RowUpdatedDate = getdate()
			from
				Staging.Fact_VesselItinerary fvi with (nolock)
			where
				fvi.VesselItineraryAlternateKey = Warehouse.Fact_VesselItinerary.VesselItineraryAlternateKey
				and fvi.RecordStatus = @ExistingRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end