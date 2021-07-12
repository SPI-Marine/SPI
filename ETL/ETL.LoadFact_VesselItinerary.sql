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
Brian Boswick	12/13/2019	Changed DateModified field to point to new DateETAWasUpdatedByOperator_BETA field
Brian Boswick	01/24/2020	Removed ETA change logic for ETALastModifiedDate
Brian Boswick	01/25/2020	Added PortOrder field logic
Brian Boswick	02/05/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	02/11/2020	Added VesselKey ETL logic
Brian Boswick	02/21/2020	Added Direction and ProductType ETL logic
Brian Boswick	05/06/2020	Added VesselPortStatusOfficial
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	10/12/2020	Added ETAEndOriginal
Brian Boswick	12/14/2020	Added EOSPStartDate to replace NORStartDate
Brian Boswick	04/29/2021	Added FirstLoadEventDateKey ETL
Brian Boswick	07/12/2021	Removed COAKey
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
																ChartererKey,
																OwnerKey,
																VesselKey,
																DirectionKey,
																FirstLoadEventDateKey,
																ItineraryPortType,
																Comments,
																NORStartDate,
																EOSPStartDate,
																ETAOriginalDate,
																ETAOriginalCreateDate,
																ETAEndOriginal,
																MostRecentETADate,
																ETALastModifiedDate,
																LoadDischarge,
																VesselPortStatus_Override,
																RelatedParcelPortID,
																RelatedPortID,
																ETAChanged,
																DateModified,
																PortOrder,
																VesselPortStatusOfficial,
																RecordStatus
															)
		select
			distinct
				vi.RecordID										VesselItineraryAlternateKey,
				isnull(fixture.PostFixtureKey, -1)				PostFixtureKey,
				-1												PortKey,
				isnull(sd.DateKey, 18991230)					ETAStartDateKey,
				coalesce(ed.DateKey, sd.DateKey, 18991230)		ETAEndDateKey,
				isnull(dm.DateKey, 47001231)					DateModifiedKey,	--- REMOVE THIS FIELD AFTER LETTING RACHEL KNOW ---
				isnull(wch.ChartererKey, -1)					ChartererKey,
				isnull(wo.OwnerKey, -1)							OwnerKey,
				isnull(v.VesselKey, -1)							VesselKey,
				-1												DirectionKey,
				-1												FirstLoadEventDateKey,
				vi.ItineraryPortType							ItineraryPortType,
				vi.Comments										Comments,
				firstnorevent.FirstNOREventDate					NORStartDate,
				firsteospevent.FirstEOSPEventDate				EOSPStartDate,
				vi.ETAStartOriginal_ADMIN						ETAOriginalDate,
				vi.OriginalETACreatedOn_ADMIN					ETAOriginalCreateDate,
				loaddischarge.ETAEndOriginal					ETAEndOriginal,
				coalesce(vi.ETAStart, wvi.MostRecentETADate)	MostRecentETADate,
				vi.DateETAWasUpdatedByOperator_BETA				ETALastModifiedDate,
				loaddischarge.[Type]							LoadDischarge,
				isnull(vi.VesselPortStatus_Override, 0)			VesselPortStatus_Override,
				vi.RelatedParcelPortID							RelatedParcelPortID,
				vi.RelatedPortID								RelatedPortID,
				case
					when convert(date, isnull(vi.ETAStart, '12/30/1899')) <> isnull(wvi.MostRecentETADate, '12/30/1899')
						then 1
					else 0
				end												ETAChanged,
				vi.DateETAWasUpdatedByOperator_BETA				DateModified,
				portorder.PortOrder								PortOrder,
				vi.VesselPortStatusOfficial						VesselPortStatusOfficial,
				isnull(rs.RecordStatus, @NewRecord)				RecordStatus
			from
				VesselItinerary vi with (nolock)
					left join Warehouse.Dim_PostFixture fixture with (nolock)
						on vi.RelatedSPIFixtureID = fixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar sd with (nolock)
						on sd.FullDate = try_convert(date, vi.ETAStart)
					left join Warehouse.Dim_Calendar ed with (nolock)
						on ed.FullDate = try_convert(date, vi.ETAEnd)
					left join Warehouse.Dim_Calendar dm with (nolock)
						on dm.FullDate = try_convert(date, vi.DateETAWasUpdatedByOperator_BETA)
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
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											min(e.StartDate)	FirstEOSPEventDate
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
													pet.QBRecId in (36, 214) -- Arrived / EOSP
												group by
													pf.QBRecId, pp.RelatedPortId
								) firsteospevent
						on firsteospevent.PostFixtureAlternateKey = vi.RelatedSPIFixtureID
							and firsteospevent.RelatedPortID = loaddischarge.RelatedPortId
					left join	(
									select
											pf.QBRecId													PostFixtureAlternateKey,
											pp.RelatedPortId											RelatedPortID,
											min(convert(date, e.StartDate))								NOREventDate,
											pp.[Type]													LoadDischarge,
											row_number() over	(
																	partition by pf.QBRecId, pp.[Type]
																	order by min(convert(date, e.StartDate))
																)										PortOrder
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
											pf.QBRecId,
											pp.RelatedPortId,
											pp.[Type]
								) portorder
						on portorder.PostFixtureAlternateKey = vi.RelatedSPIFixtureID
							and portorder.RelatedPortID = loaddischarge.RelatedPortId
					left join Warehouse.Fact_VesselItinerary wvi with (nolock)
						on wvi.VesselItineraryAlternateKey = vi.RecordID
					left join PostFixtures pf with (nolock)
						on vi.RelatedSpiFixtureId = pf.QBRecId
					left join FullStyles fs with (nolock)
						on pf.RelatedChartererFullStyle = fs.QBRecId
					left join Warehouse.Dim_Owner wo with (nolock)
						on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
					left join Warehouse.Dim_Charterer wch with (nolock)
						on wch.ChartererAlternateKey = fs.RelatedChartererParentID
					left join Warehouse.Dim_Vessel v with (nolock)
						on v.VesselAlternateKey = pf.RelatedVessel
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
				DaysBetweenRecentETALastModified = try_convert(smallint, abs(datediff(day, MostRecentETADate, ETALastModifiedDate)));

	end try
	begin catch
		select @ErrorMsg = 'Updating DaysBetweenRecentETALastModified - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
		
	-- Update One and Two Week ETAs
	begin try
		update
				svi with (tablock)
			set
				OneWeekETA =	case
									when isnull(wvi.OneWeekETA, '12/30/1899') = '12/30/1899'
											and svi.DaysBetweenRecentETALastModified between 6 and 9
										then svi.MostRecentETADate
									else wvi.OneWeekETA
								end,
				TwoWeekETA =	case
									when isnull(wvi.TwoWeekETA, '12/30/1899') = '12/30/1899'
											and svi.DaysBetweenRecentETALastModified between 13 and 16
										then svi.MostRecentETADate
									else wvi.TwoWeekETA
								end
			from
				Staging.Fact_VesselItinerary svi
					join Warehouse.Fact_VesselItinerary wvi
						on svi.VesselItineraryAlternateKey = wvi.VesselItineraryAlternateKey;

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
				DaysOutOriginalETASent = try_convert(smallint, abs(datediff(day, EOSPStartDate, ETAOriginalCreateDate))),
				DaysOutOriginalETA = try_convert(smallint, abs(datediff(day, EOSPStartDate, ETAOriginalDate))),
				DaysOutTwoWeekETA = try_convert(smallint, abs(datediff(day, EOSPStartDate, TwoWeekETA))),
				DaysOutOneWeekETA = try_convert(smallint, abs(datediff(day, EOSPStartDate, OneWeekETA)))
			where
				isnull(NORStartDate, '12/30/1899') > '12/30/1899';

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
				ArrivedGreaterThanSevenDaysOneWeek	=	case when DaysOutOneWeekETA > 7 then 1 else null end;

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

	-- Update FirstLoadEventDateKey
	begin try
		update
				Staging.Fact_VesselItinerary
			set
				FirstLoadEventDateKey = fnd.DateKey
			from
				Staging.FirstLoadPortNORDates firstnor
					join Warehouse.Dim_Calendar fnd
						on fnd.FullDate = firstnor.FirstNOR
			where
				firstnor.PostFixtureKey = Staging.Fact_VesselItinerary.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstLoadEventDateKey - ' + error_message();
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
		
	-- Update Direction by pulling last Discharge Itinerary entry for the Post Fixture
	begin try
		with
			LastDischargePort	(
									PostFixtureKey,
									PortKey
								)
		as
		(
			select
					PostFixtureKey,
					PortKey
				from
					Staging.Fact_VesselItinerary vi with (nolock)
				where
					vi.LoadDischarge = 'Discharge'			
					and vi.ETAStartDateKey =	(
													select
															max(ETAStartDateKey) MaxDate
														from
															Staging.Fact_VesselItinerary with (nolock)
														where
															PostFixtureKey = vi.PostFixtureKey
															and ItineraryPortType = 'Fixture Port'
												)
								
			
		)

		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				Direction = wlp.Area,
				DirectionKey = lp.PortKey
			from
				LastDischargePort lp
					join Warehouse.Dim_Port wlp
						on lp.PortKey = wlp.PortKey							
			where
				LoadDischarge = 'Load'
				and Staging.Fact_VesselItinerary.PostFixtureKey = lp.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating Direction for Load Ports - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ProductType by using the ProductType of the largest quantity Parcel
	begin try
		with
			MaxProductType	(
								PostFixtureKey,
								ProductType
							)
		as
		(
			select
					pf.PostFixtureKey,
					pt.TypeName ProductType					
				from
					ParcelProducts pp with (nolock)
						join Warehouse.Dim_PostFixture pf with (nolock)
							on pf.PostFixtureAlternateKey = pp.RelatedSPIFixtureId
						join Products p with (nolock)
							on pp.RelatedProductId = p.QBRecId
						join ProductType pt with (nolock)
							on pt.QBRecId = p.RelatedProductTypeId
						join Parcels parcel with (nolock)
							on pp.QBRecId = parcel.RelatedParcelProductId
				where
					parcel.BLQty =	(
										select
												max(par.BLQty)
											from
												Parcels par with (nolock)
											where
												par.RelatedSpiFixtureId = parcel.RelatedSpiFixtureId
									)
		)

		update
				Staging.Fact_VesselItinerary with (tablock)
			set
				ProductType = mpt.ProductType
			from
				MaxProductType mpt
			where
				mpt.PostFixtureKey = Staging.Fact_VesselItinerary.PostFixtureKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating ProductType records - ' + error_message();
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
																	ChartererKey,
																	OwnerKey,
																	VesselKey,
																	DirectionKey,
																	FirstLoadEventDateKey,
																	ItineraryPortType,
																	Comments,
																	NORStartDate,
																	EOSPStartDate,
																	ETAOriginalDate,
																	ETAOriginalCreateDate,
																	ETAEndOriginal,
																	TwoWeekETA,
																	OneWeekETA,
																	MostRecentETADate,
																	ETALastModifiedDate,
																	LoadDischarge,
																	NORWithinLaycanOriginal,
																	NORWithinLaycanFinal,
																	ETAWithinLaycanOriginal,
																	ETAWithinLaycanFinal,
																	PortOrder,
																	Direction,
																	ProductType,
																	VesselPortStatusOfficial,
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
																	VesselPortStatus_Override,
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
					fvi.ChartererKey,
					fvi.OwnerKey,
					fvi.VesselKey,
					fvi.DirectionKey,
					fvi.FirstLoadEventDateKey,
					fvi.ItineraryPortType,
					fvi.Comments,
					fvi.NORStartDate,
					fvi.EOSPStartDate,
					fvi.ETAOriginalDate,
					fvi.ETAOriginalCreateDate,
					fvi.ETAEndOriginal,
					fvi.TwoWeekETA,
					fvi.OneWeekETA,
					fvi.MostRecentETADate,
					fvi.ETALastModifiedDate,
					fvi.LoadDischarge,
					fvi.NORWithinLaycanOriginal,
					fvi.NORWithinLaycanFinal,
					fvi.ETAWithinLaycanOriginal,
					fvi.ETAWithinLaycanFinal,
					fvi.PortOrder,
					fvi.Direction,
					fvi.ProductType,
					fvi.VesselPortStatusOfficial,
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
					fvi.VesselPortStatus_Override,
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
				ChartererKey = fvi.ChartererKey,
				OwnerKey = fvi.OwnerKey,
				VesselKey = fvi.VesselKey,
				DirectionKey = fvi.DirectionKey,
				FirstLoadEventDateKey = fvi.FirstLoadEventDateKey,
				ItineraryPortType = fvi.ItineraryPortType,
				Comments = fvi.Comments,
				NORStartDate = fvi.NORStartDate,
				EOSPStartDate = fvi.EOSPStartDate,
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
				VesselPortStatus_Override = fvi.VesselPortStatus_Override,
				PortOrder = fvi.PortOrder,
				Direction = fvi.Direction,
				ProductType = fvi.ProductType,
				VesselPortStatusOfficial = fvi.VesselPortStatusOfficial,
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

	-- Remove deleted source records
	begin try
		delete
				Warehouse.Fact_VesselItinerary with (tablock)
			where
				not exists	(
								select
										1
									from
										VesselItinerary vi
									where
										vi.RecordID = Warehouse.Fact_VesselItinerary.VesselItineraryAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end