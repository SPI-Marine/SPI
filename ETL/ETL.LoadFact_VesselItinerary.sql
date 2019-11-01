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
Brian Boswick	10/30/2019	Added Within Laycan metrics
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
				Staging.Fact_VesselItinerary with (tablock)
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
		select @ErrorMsg = 'Updating ETALastModifiedDate/MostRecentETADate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update DaysBetweenRecentETALastModified
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				DaysBetweenRecentETALastModified = try_convert(smallint, abs(datediff(day, vi.MostRecentETADate, vi.ETALastModifiedDate)))
			from
				Staging.Fact_VesselItinerary vi;

	end try
	begin catch
		select @ErrorMsg = 'Updating DaysBetweenRecentETALastModified - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update One and Two Week ETAs
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
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
		select @ErrorMsg = 'Updating One and Two Week ETAs - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update Days Out bucket metrics
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				DaysOutOriginalETASent = try_convert(smallint, abs(datediff(day, NORStartDate, ETAOriginalCreateDate))),
				DaysOutOriginalETA = try_convert(smallint, abs(datediff(day, NORStartDate, ETAOriginalDate))),
				DaysOutTwoWeekETA = try_convert(smallint, abs(datediff(day, NORStartDate, TwoWeekETA))),
				DaysOutOneWeekETA = try_convert(smallint, abs(datediff(day, NORStartDate, OneWeekETA)))
			from
				Staging.Fact_VesselItinerary vi
			where
				isnull(vi.NORStartDate, '12/30/1899') > '12/30/1899';

	end try
	begin catch
		select @ErrorMsg = 'Updating DaysOut bucket metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update Days Out buckets flags
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				ArrivedLessThanThreeDaysOriginal	=	case when DaysOutOriginalETA < 3 then 1 else null end,
				ArrivedThreeToSevenDaysOriginal		=	case when DaysOutOriginalETA between 3 and 7 then 1 else null end,
				ArrivedGreaterThanSevenDaysOriginal	=	case when DaysOutOriginalETA > 7 then 1 else null end,
				ArrivedLessThanThreeDaysTwoWeek		=	case when DaysOutTwoWeekETA < 3 then 1 else null end,
				ArrivedThreeToSevenDaysTwoWeek		=	case when DaysOutTwoWeekETA between 3 and 7 then 1 else null end,
				ArrivedGreaterThanSevenDaysTwoWeek	=	case when DaysOutTwoWeekETA > 7 then 1 else null end,
				ArrivedLessThanThreeDaysOneWeek		=	case when DaysOutOneWeekETA < 3 then 1 else null end,
				ArrivedThreeToSevenDaysOneWeek		=	case when DaysOutOneWeekETA between 3 and 7 then 1 else null end,
				ArrivedGreaterThanSevenDaysOneWeek	=	case when DaysOutOneWeekETA > 7 then 1 else null end
			from
				Staging.Fact_VesselItinerary vi;

	end try
	begin catch
		select @ErrorMsg = 'Updating DaysOut bucket flags - ' + error_message();
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
	
	-- Calculate aggregate NominatedQuantity
	begin try
		with
			LoadPortQuantities(PostFixtureKey, PortKey, NominatedQuantity)
			as
				(
					select
							wpf.PostFixtureKey,
							wlp.PortKey,
							sum(p.NominatedQty) NominatedQuantity
						from
							Parcels p with (nolock)
								join ParcelPorts loadport with (nolock)
									on p.RelatedLoadPortID = loadport.QBRecId
								join Warehouse.Dim_Port wlp with (nolock)
									on loadport.RelatedPortId = wlp.PortAlternateKey
								join Warehouse.Dim_PostFixture wpf with (nolock)
									on p.RelatedSpiFixtureId = wpf.PostFixtureAlternateKey
						group by
							wpf.PostFixtureKey,
							wlp.PortKey
				),
			DischargePortQuantities(PostFixtureKey, PortKey, NominatedQuantity)
			as
				(
					select
							wpf.PostFixtureKey,
							wdp.PortKey,
							sum(p.NominatedQty) NominatedQuantity
						from
							Parcels p with (nolock)
								join ParcelPorts dischargeport with (nolock)
									on p.RelatedDischPortId = dischargeport.QBRecId
								join Warehouse.Dim_Port wdp with (nolock)
									on dischargeport.RelatedPortId = wdp.PortAlternateKey
								join Warehouse.Dim_PostFixture wpf with (nolock)
									on p.RelatedSpiFixtureId = wpf.PostFixtureAlternateKey
						group by
							wpf.PostFixtureKey,
							wdp.PortKey
				)

		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				NominatedQuantity = coalesce(lpq.NominatedQuantity, dpq.NominatedQuantity)
			from
				Staging.Fact_VesselItinerary vi
					left join LoadPortQuantities lpq with (nolock)
						on lpq.PostFixtureKey = vi.PostFixtureKey
							and lpq.PortKey = vi.PortKey
							and vi.LoadDischarge = 'Load'
					left join DischargePortQuantities dpq with (nolock)
						on dpq.PostFixtureKey = vi.PostFixtureKey
							and dpq.PortKey = vi.PortKey
							and vi.LoadDischarge = 'Discharge';

	end try
	begin catch
		select @ErrorMsg = 'Updating aggregate NominatedQuantity - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Find first load port NORs
	begin try
		drop table if exists Staging.FirstLoadPortNORDates;
		create table Staging.FirstLoadPortNORDates	(
														PostFixtureKey	int,
														PortKey			int,
														FirstNOR		date
													);
	
		insert
				Staging.FirstLoadPortNORDates
			select
					wpf.PostFixtureKey,
					wlp.PortKey,
					min(convert(date, evt.StartDate)) FirstNOR
				from
					SOFEvents evt with (nolock)
						join ParcelBerths pb with (nolock)
							on pb.QBRecId = evt.RelatedParcelBerthId
						join ParcelPorts loadport with (nolock)
							on pb.RelatedLDPId = loadport.QBRecId
						join Warehouse.Dim_Port wlp with (nolock)
							on loadport.RelatedPortId = wlp.PortAlternateKey
						join Warehouse.Dim_PostFixture wpf with (nolock)
							on pb.RelatedSpiFixtureId = wpf.PostFixtureAlternateKey
				where
					evt.RelatedPortTimeEventId = 219 -- NOR Tendered
					and loadport.[Type] = 'Load'
				group by
					wpf.PostFixtureKey,
					wlp.PortKey
				having
					count(wpf.PostFixtureKey) = 1;		-- Avoid post fixtures that have duplicate NOR Start Dates
	end try
	begin catch
		select @ErrorMsg = 'Finding first load port NORs - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update NORWithinLaycan flags
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				NORWithinLaycanOriginal =	case
												when nd.FirstNOR between convert(date, pf.LaycanCommencementOriginal)
														and convert(date, pf.LaycanCancellingOriginal)
													then 1
												else null
											end,
				NORWithinLaycanFinal =	case
											when nd.FirstNOR between convert(date, pf.LaycanCommencementFinal_QBC)
													and convert(date, pf.LaycanCancellingFinal_QBC)
												then 1
											else null
										end
			from
				Staging.FirstLoadPortNORDates nd with (nolock)
					join Warehouse.Dim_PostFixture pf with (nolock)
						on pf.PostFixtureKey = nd.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'NORWithinLaycan flags - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update ETAWithinLaycan flags
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				ETAWithinLaycanOriginal =	case
											when MostRecentETADate between convert(date, pf.LaycanCommencementOriginal)
													and convert(date, pf.LaycanCancellingOriginal)
												then 1
											else null
										end,
				ETAWithinLaycanFinal =	case
											when MostRecentETADate between convert(date, pf.LaycanCommencementFinal_QBC)
													and convert(date, pf.LaycanCancellingFinal_QBC)
												then 1
											else null
										end
			from
				Warehouse.Dim_PostFixture pf with (nolock)
			where
				pf.PostFixtureKey = Staging.Fact_VesselItinerary.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'ETAWithinLaycan flags - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update NORLaycanOverUnder metrics
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				NORLaycanOverUnderOriginal =	case
													when nd.FirstNOR <= convert(date, pf.LaycanCommencementOriginal)
														then datediff(day, convert(date, pf.LaycanCommencementOriginal), nd.FirstNOR)
													when nd.FirstNOR > convert(date, pf.LaycanCancellingOriginal)
														then datediff(day, convert(date, pf.LaycanCancellingOriginal), nd.FirstNOR)
													else null
												end,
				NORLaycanOverUnderFinal =	case
												when nd.FirstNOR <= convert(date, pf.LaycanCommencementFinal_QBC)
													then datediff(day, convert(date, pf.LaycanCommencementFinal_QBC), nd.FirstNOR)
												when nd.FirstNOR > convert(date, pf.LaycanCancellingFinal_QBC)
													then datediff(day, convert(date, pf.LaycanCancellingFinal_QBC), nd.FirstNOR)
												else null
											end
			from
				Staging.FirstLoadPortNORDates nd with (nolock)
					join Warehouse.Dim_PostFixture pf with (nolock)
						on pf.PostFixtureKey = nd.PostFixtureKey
			where
				nd.PostFixtureKey = Staging.Fact_VesselItinerary.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'NORLaycanOverUnder metrics - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update ETALaycanOverUnder metrics
	begin try
		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				ETALaycanOverUnderOriginal =	case
													when MostRecentETADate <= convert(date, pf.LaycanCommencementOriginal)
														then datediff(day, convert(date, pf.LaycanCommencementOriginal), MostRecentETADate)
													when MostRecentETADate > convert(date, pf.LaycanCancellingOriginal)
														then datediff(day, convert(date, pf.LaycanCancellingOriginal), MostRecentETADate)
													else null
												end,
				ETALaycanOverUnderFinal =	case
												when MostRecentETADate <= convert(date, pf.LaycanCommencementFinal_QBC)
													then datediff(day, convert(date, pf.LaycanCommencementFinal_QBC), MostRecentETADate)
												when MostRecentETADate > convert(date, pf.LaycanCancellingFinal_QBC)
													then datediff(day, convert(date, pf.LaycanCancellingFinal_QBC), MostRecentETADate)
												else null
											end
			from
				Warehouse.Dim_PostFixture pf with (nolock)
			where
				pf.PostFixtureKey = Staging.Fact_VesselItinerary.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'ETALaycanOverUnder metrics - ' + error_message();
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
																	NORWithinLaycanOriginal,
																	NORWithinLaycanFinal,
																	ETAWithinLaycanOriginal,
																	ETAWithinLaycanFinal,
																	NORLaycanOverUnderOriginal,
																	NORLaycanOverUnderFinal,
																	ETALaycanOverUnderOriginal,
																	ETALaycanOverUnderFinal,
																	DaysBetweenRecentETALastModified,
																	DaysOutOriginalETASent,
																	DaysOutOriginalETA,
																	DaysOutTwoWeekETA,
																	DaysOutOneWeekETA,
																	ArrivedLessThanThreeDaysOriginal,
																	ArrivedThreeToSevenDaysOriginal,
																	ArrivedGreaterThanSevenDaysOriginal,
																	ArrivedLessThanThreeDaysTwoWeek,
																	ArrivedThreeToSevenDaysTwoWeek,
																	ArrivedGreaterThanSevenDaysTwoWeek,
																	ArrivedLessThanThreeDaysOneWeek,
																	ArrivedThreeToSevenDaysOneWeek,
																	ArrivedGreaterThanSevenDaysOneWeek,
																	NominatedQuantity,
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
					fvi.NORWithinLaycanOriginal,
					fvi.NORWithinLaycanFinal,
					fvi.ETAWithinLaycanOriginal,
					fvi.ETAWithinLaycanFinal,
					fvi.NORLaycanOverUnderOriginal,
					fvi.NORLaycanOverUnderFinal,
					fvi.ETALaycanOverUnderOriginal,
					fvi.ETALaycanOverUnderFinal,
					fvi.DaysBetweenRecentETALastModified,
					fvi.DaysOutOriginalETASent,
					fvi.DaysOutOriginalETA,
					fvi.DaysOutTwoWeekETA,
					fvi.DaysOutOneWeekETA,
					fvi.ArrivedLessThanThreeDaysOriginal,
					fvi.ArrivedThreeToSevenDaysOriginal,
					fvi.ArrivedGreaterThanSevenDaysOriginal,
					fvi.ArrivedLessThanThreeDaysTwoWeek,
					fvi.ArrivedThreeToSevenDaysTwoWeek,
					fvi.ArrivedGreaterThanSevenDaysTwoWeek,
					fvi.ArrivedLessThanThreeDaysOneWeek,
					fvi.ArrivedThreeToSevenDaysOneWeek,
					fvi.ArrivedGreaterThanSevenDaysOneWeek,
					fvi.NominatedQuantity,
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
				DaysBetweenRecentETALastModified = fvi.DaysBetweenRecentETALastModified,
				DaysOutOriginalETASent = fvi.DaysOutOriginalETASent,
				DaysOutOriginalETA = fvi.DaysOutOriginalETA,
				DaysOutTwoWeekETA = fvi.DaysOutTwoWeekETA,
				DaysOutOneWeekETA = fvi.DaysOutOneWeekETA,
				ArrivedLessThanThreeDaysOriginal = fvi.ArrivedLessThanThreeDaysOriginal,
				ArrivedThreeToSevenDaysOriginal = fvi.ArrivedThreeToSevenDaysOriginal,
				ArrivedGreaterThanSevenDaysOriginal = fvi.ArrivedGreaterThanSevenDaysOriginal,
				ArrivedLessThanThreeDaysTwoWeek = fvi.ArrivedLessThanThreeDaysTwoWeek,
				ArrivedThreeToSevenDaysTwoWeek = fvi.ArrivedThreeToSevenDaysTwoWeek,
				ArrivedGreaterThanSevenDaysTwoWeek = fvi.ArrivedGreaterThanSevenDaysTwoWeek,
				ArrivedLessThanThreeDaysOneWeek = fvi.ArrivedLessThanThreeDaysOneWeek,
				ArrivedThreeToSevenDaysOneWeek = fvi.ArrivedThreeToSevenDaysOneWeek,
				ArrivedGreaterThanSevenDaysOneWeek = fvi.ArrivedGreaterThanSevenDaysOneWeek,
				NominatedQuantity = fvi.NominatedQuantity,
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