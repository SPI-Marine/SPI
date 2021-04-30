set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_FixtureBerth;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/06/2019
Description:	Creates the LoadFact_FixtureBerth stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	06/26/2019	Added TimeToCountCommenceLoad_CompleteLoad, TimeToCountCommenceDischarge_CompleteDischarge,
							AverageTimeToCountCommenceLoad_CompleteLoad and 
							AverageTimeToCountCommenceDischarge_CompleteDischarge metrics
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	02/10/2020	Added ProductKey ETL logic
Brian Boswick	02/12/2020	Added ProductQuantityKey ETL logic
Brian Boswick	02/13/2020	Renamed multiple metrics
Brian Boswick	04/22/2020	Added CPDateKey ETL
Brian Boswick	06/02/2020	Pull event duration from Staging.SOFEvent_Durations table
Brian Boswick	07/29/2020	Added COAKey
Brian Boswick	08/19/2020	Added DischargePortBerthKey, LoadBerthKey
Brian Boswick	08/21/2020	Renamed ProductQuantityKey to ProductFixtureBerthQuantityKey
Brian Boswick	08/28/2020	Added LoadPortBerthKey
Brian Boswick	09/18/2020	Removed Pump Rates > 2500 from average calcualtion
Brian Boswick	10/22/2020	Added BerthPumpRate
Brian Boswick	11/05/2020	Modified ETL for new quantity ranges
==========================================================================================================	
*/

create procedure ETL.LoadFact_FixtureBerth
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging tables
	if object_id(N'Staging.Fact_FixtureBerth', 'U') is not null
		truncate table Staging.Fact_FixtureBerth;

	if object_id(N'Staging.Fact_FixtureBerthEvents', 'U') is not null
		truncate table Staging.Fact_FixtureBerthEvents;

	-- Get Unique FixtureBerth records
	begin try
		with LoadPortBerths	(
								PostFixtureAlternateKey,
								PortAlternateKey,
								BerthAlternateKey,
								LoadDischargeAlternateKey,
								ParcelAlternateKey,
								ParcelBerthAlternateKey,
								ParcelQuantity,
								LoadDischarge,
								LaytimeAllowed
							)
		as
		(
			select
				distinct
					p.RelatedSpiFixtureId,
					loadport.RelatedPortId,
					loadberth.RelatedBerthId,
					loadport.QBRecId,
					p.QbRecId,
					loadberth.QBRecId,
					p.BLQty,
					loadport.[Type],
					loadberth.LaytimeAllowedBerthHrs_QBC
				from
					Parcels p with (nolock)
						join ParcelBerths loadberth with (nolock)
							on p.RelatedLoadBerth = loadberth.QBRecId
						join ParcelPorts loadport with (nolock)
							on loadberth.RelatedLDPId = loadport.QBRecId
				where
					p.RelatedSpiFixtureId is not null
		),
		DischPortBerths	(
							PostFixtureAlternateKey,
							PortAlternateKey,
							BerthAlternateKey,
							LoadDischargeAlternateKey,
							ParcelAlternateKey,
							ParcelBerthAlternateKey,
							ParcelQuantity,
							LoadDischarge,
							LaytimeAllowed
						)
		as
		(
			select
				distinct
					p.RelatedSpiFixtureId,
					dischport.RelatedPortId,
					dischberth.RelatedBerthId,
					dischport.QBRecId,
					p.QbRecId,
					dischberth.QBRecId,
					p.BLQty,
					dischport.[Type],
					dischberth.LaytimeAllowedBerthHrs_QBC
				from
					Parcels p
						join ParcelBerths dischberth with (nolock)
							on p.RelatedDischBerth = dischberth.QBRecId
						join ParcelPorts dischport with (nolock)
							on dischberth.RelatedLDPId = dischport.QBRecId
				where
					p.RelatedSpiFixtureId is not null
		),
		UniqueFixtureBerths	(
								PostFixtureAlternateKey,
								PortAlternateKey,
								BerthAlternateKey,
								LoadDischargeAlternateKey,
								ParcelAlternateKey,
								ParcelBerthAlternateKey,
								ParcelQuantity,
								LoadDischarge,
								LaytimeAllowed
							)
		as
		(
			select * from LoadPortBerths
			union all
			select * from DischPortBerths
		),
		AggregatedParcelQuantity	(
										PostFixtureAlternateKey,
										PortAlternateKey,
										BerthAlternateKey,
										LoadDischargeAlternateKey,
										ParcelBerthAlternateKey,
										ParcelQuantity,
										LoadDischarge,
										LaytimeAllowed
									)
		as
		(
			select 
					fb.PostFixtureAlternateKey,
					fb.PortAlternateKey,
					fb.BerthAlternateKey,
					fb.LoadDischargeAlternateKey,
					fb.ParcelBerthAlternateKey,
					sum(fb.ParcelQuantity) ParcelQuantity,
					fb.LoadDischarge,
					fb.LaytimeAllowed
				from
					UniqueFixtureBerths fb 
				group by
					fb.PostFixtureAlternateKey,
					fb.PortAlternateKey,
					fb.BerthAlternateKey,
					fb.LoadDischargeAlternateKey,
					fb.ParcelBerthAlternateKey,
					fb.LoadDischarge,
					fb.LaytimeAllowed		
		)

		insert
				Staging.Fact_FixtureBerth with (tablock)
											(
												PostFixtureAlternateKey,
												PortAlternateKey,
												BerthAlternateKey,
												LoadDischargeAlternateKey,
												ParcelBerthAlternateKey,
												LoadPortBerthKey,
												DischargePortBerthKey,
												PortBerthKey,
												PostFixtureKey,
												VesselKey,
												FirstEventDateKey,
												CPDateKey,
												LoadPortKey,
												LoadBerthKey,
												DischargePortKey,
												ChartererKey,
												OwnerKey,
												ProductKey,
												ProductFixtureBerthQuantityKey,
												COAKey,
												LoadDischarge,
												ParcelQuantity,
												LaytimeAllowed
											)
			select
				distinct
					ufb.PostFixtureAlternateKey								PostFixtureAlternateKey,
					isnull(ufb.PortAlternateKey, -1)						PortAlternateKey,
					isnull(ufb.BerthAlternateKey, -1)						BerthAlternateKey,
					ufb.LoadDischargeAlternateKey							LoadDischargeAlternateKey,
					isnull(ufb.ParcelBerthAlternateKey, -1)					ParcelBerthAlternateKey,
					isnull(loadportberth.PortBerthKey, -1)					LoadPortBerthKey,
					isnull(dischportberth.PortBerthKey, -1)					DischargePortBerthKey,
					isnull(portberth.PortBerthKey, -1)						PortBerthKey,
					isnull(wpostfixture.PostFixtureKey, -1)					PostFixtureKey,
					isnull(vessel.VesselKey, -1)							VesselKey,
					-1														FirstEventDateKey,
					isnull(CPDate.DateKey, -1)								CPDateKey,
					isnull(wloadport.PortKey, -1)							LoadPortKey,
					isnull(loadberth.BerthKey, -1)							LoadBerthKey,
					isnull(wdischport.PortKey, -1)							DischargePortKey,
					isnull(wch.ChartererKey, -1)							ChartererKey,
					isnull(wo.OwnerKey, -1)									OwnerKey,
					-1														ProductKey,
					isnull(pq.ProductQuantityKey, -1)						ProductFixtureBerthQuantityKey,
					isnull(coa.COAKey, -1)									COAKey,
					ufb.LoadDischarge										LoadDischarge,
					ufb.ParcelQuantity										ParcelQuantity,
					ufb.LaytimeAllowed										LaytimeAllowed
				from
					AggregatedParcelQuantity ufb
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = ufb.PostFixtureAlternateKey
						left join Warehouse.Dim_Calendar CPDate with (nolock)
							on CPDate.FullDate = wpostfixture.CPDate
						left join PostFixtures epostfixture with (nolock)
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_COA coa (nolock)
							on coa.COAAlternateKey = epostfixture.RelatedSPICOAId
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join ParcelPorts pp with (nolock)
							on pp.QBRecId = ufb.LoadDischargeAlternateKey
						left join Warehouse.Dim_Port wloadport with (nolock)
							on wloadport.PortAlternateKey = pp.RelatedPortId
								and pp.[Type] = 'Load'
						left join Warehouse.Dim_Port wdischport with (nolock)
							on wdischport.PortAlternateKey = pp.RelatedPortId
								and pp.[Type] = 'Discharge'
						left join Warehouse.Dim_PortBerth portberth with (nolock)
							on portberth.PortAlternateKey = ufb.PortAlternateKey
								and portberth.BerthAlternateKey = ufb.BerthAlternateKey
						left join Warehouse.Dim_PortBerth loadportberth with (nolock)
							on loadportberth.PortAlternateKey = ufb.PortAlternateKey
								and loadportberth.BerthAlternateKey = ufb.BerthAlternateKey
								and ufb.LoadDischarge = 'Load'
						left join Warehouse.Dim_PortBerth dischportberth with (nolock)
							on dischportberth.PortAlternateKey = ufb.PortAlternateKey
								and dischportberth.BerthAlternateKey = ufb.BerthAlternateKey
								and ufb.LoadDischarge = 'Discharge'
						left join Warehouse.Dim_Berth loadberth with (nolock)
							on loadberth.BerthAlternateKey = ufb.BerthAlternateKey
								and ufb.LoadDischarge = 'Load'
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID
						left join Warehouse.Dim_ProductQuantity pq with (nolock)
							on convert(decimal(18, 4), ufb.ParcelQuantity) >= pq.MinimumQuantity
								and convert(decimal(18, 4), ufb.ParcelQuantity) < pq.MaximumQuantity
				where
					isnull(fs.FullStyleName, '') <> 'ABC Charterer'
					and wpostfixture.FixtureStatusCategory <> 'Cancelled';
	end try
	begin catch
		select @ErrorMsg = 'Staging FixtureBerth records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Update ProductType/ProductKey for the Post Fixture
	begin try
		with TopProductTypeQuantities	(
											PostFixtureAlternateKey,
											MaxQuantity,
											ProductType,
											ProductKey
										)
		as
		(
			select
					p.RelatedSpiFixtureId	PostFixtureAlternateKey,
					max(p.BLQty)			MaxQuantity,
					prodtype.TypeName		ProductType,
					wp.ProductKey			ProductKey
				from
					Parcels p with (nolock)
						join ParcelProducts pp with (nolock)
							on p.RelatedParcelProductId = pp.QBRecId
						join Products prod with (nolock)
							on pp.RelatedProductId = prod.QBRecId
						join ProductType prodtype with (nolock)
							on prod.RelatedProductTypeId = prodtype.QBRecId
						join Warehouse.Dim_Product wp with (nolock)
							on wp.ProductAlternateKey = prod.QBRecId
				where
					p.RelatedSpiFixtureId is not null
				group by
					p.RelatedSpiFixtureId,
					prodtype.TypeName,
					wp.ProductKey
		)
				
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				ProductType = prodtype.ProductType,
				ProductKey = prodtype.ProductKey
			from
				TopProductTypeQuantities prodtype						
			where
				prodtype.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating ProductKey - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Retrieve event durations
	begin try
		with EventDurations	(
								PostFixtureAlternateKey,
								ParcelBerthAlternateKey,
								EventAlternateKey,
								EventTypeId,
								EventType,
								Duration,
								LaytimeUsedProrated,
								StartDateTime,
								LoadDischarge,
								IsPumpingTime,
								IsLayTime
							)
		as
		(
			select
				distinct
					wpf.PostFixtureAlternateKey,
					ed.ParcelBerthAlternateKey,
					ed.EventAlternateKey,
					ed.EventTypeId,
					ed.EventType,
					ed.IntraEventDuration,
					ed.LaytimeUsedProrated,
					ed.EventStartDateTime,
					ed.LoadDischarge,
					ed.IsPumpingTime,
					ed.IsLaytime
				from
					Staging.SOFEvent_Durations ed with (nolock)
						join Warehouse.Dim_PostFixture wpf with (nolock)
							on wpf.PostFixtureAlternateKey = ed.PostFixtureAlternateKey
				where
					ed.PostFixtureAlternateKey > 0
		),
		OrderedEvents	(
							PostFixtureAlternateKey,
							ParcelBerthAlternateKey,
							EventAlternateKey,
							EventNum,
							EventTypeId,
							EventName,
							NextEventId,
							NextEventName,
							Duration,
							LaytimeUsedProrated,
							StartDateTime,
							LoadDischarge,
							IsPumpingTime,
							IsLayTime
						)
		as
		(
			select
					ed.PostFixtureAlternateKey								PostFixtureAlternateKey,
					ed.ParcelBerthAlternateKey								ParcelBerthAlternateKey,
					ed.EventAlternateKey,
					row_number() over	(
											partition by ed.ParcelBerthAlternateKey
											order by ed.StartDateTime, EventAlternateKey
										)									EventNum,
					ed.EventTypeId,
					ed.EventType,
					lead(ed.EventTypeId, 1, 0) over	(
														partition by ed.ParcelBerthAlternateKey
														order by ed.StartDateTime
													)			NextEventId,
					lead(ed.EventType, 1, 0) over	(
														partition by ed.ParcelBerthAlternateKey
														order by ed.StartDateTime
													)					NextEventName,
					ed.Duration,
					ed.LaytimeUsedProrated,
					ed.StartDateTime,
					ed.LoadDischarge,
					ed.IsPumpingTime,
					ed.IsLayTime
				from
					EventDurations ed
		)
		
		insert
				Staging.Fact_FixtureBerthEvents with (tablock)
			select
					oe.PostFixtureAlternateKey,
					oe.ParcelBerthAlternateKey,
					oe.LoadDischarge,
					oe.EventNum,
					oe.EventTypeId,
					oe.EventName,
					oe.NextEventId,
					oe.NextEventName,
					oe.StartDateTime,
					oe.Duration,
					oe.LaytimeUsedProrated,
					oe.IsPumpingTime,
					oe.IsLayTime
				from
					OrderedEvents oe;
	end try
	begin catch
		select @ErrorMsg = 'Staging FixtureBerthEvent Durations - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FirstEventDateKey
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FirstEventDateKey = wc.DateKey
			from
				(
					select
							min(convert(date, e.StartDateTime)) FirstEventDate,
							e.PostFixtureAlternateKey PostFixtureAlternateKey
						from
							Staging.Fact_FixtureBerthEvents e with (nolock)
						where
							e.LoadDischarge = 'Load'
						group by
							e.PostFixtureAlternateKey
				) fe
					join Warehouse.Dim_Calendar wc with (nolock)
						on wc.FullDate = fe.FirstEventDate
			where
				fe.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstEventDateKey - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationNOR_Berth
	begin try
		update
				fb with (tablock)
			set
				DurationNOR_Berth = isnull(abs(datediff(minute, e1.StartDateTime, e2.StartDateTime)/60.0), 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											*
										from
											Staging.Fact_FixtureBerthEvents (nolock)
										where
											EventTypeId = 219	-- NOR Tendered
								) e1
						on fb.PostFixtureAlternateKey = e1.PostFixtureAlternateKey
							and fb.ParcelBerthAlternateKey = e1.ParcelBerthAlternateKey		
					left join	(
									select
											*
										from
											Staging.Fact_FixtureBerthEvents (nolock)
										where
											EventTypeId = 228	-- Berth/Alongside
								) e2
						on fb.PostFixtureAlternateKey = e2.PostFixtureAlternateKey
							and fb.ParcelBerthAlternateKey = e2.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update POB_Berth. Pilot on Board to Berth. Used to calculate transit time when there is only one POB event on the first berth
	begin try
		update
				fb with (tablock)
			set
				POB_Berth = isnull(fbed.DurationPOB_Berth, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationPOB_Berth,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 225	-- Pilot On Board
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 228	-- Berth/Alongside
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating POB_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LastDropAnchor_POB. Used to calculate transit time when there is more than one POB event on the first berth.  Subtracted from the POB_Berth
	begin try
		update
				fb with (tablock)
			set
				LastDropAnchor_POB = isnull(fbed.DurationLastDropAnchor_POB, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationLastDropAnchor_POB,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			max(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 215	-- Drop Anchor
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 225	-- Pilot on Board
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating LastDropAnchor_POB - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update HasMultipleDropAnchors. Used to help determine when to subtract out LastDropAnchor_POB from POB_Berth for transit time
	begin try
		update
				fb
			set
				HasMultipleDropAnchors = 1
			from
				Staging.Fact_FixtureBerth fb
			where
				(
					select
							count(*)
						from
							Staging.Fact_FixtureBerthEvents fbe with (nolock)
						where
							fbe.EventTypeId = 225
							and fbe.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and fbe.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey
				) > 1
	end try
	begin catch
		select @ErrorMsg = 'Updating HasMultipleDropAnchors - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Find FirstFixtureBerth for transit time calculation
	begin try
		update
				fb with (tablock)
			set
				FirstFixtureBerth =	case
										when isnull(fe.ParcelBerthAlternateKey, 0) = fb.ParcelBerthAlternateKey
											then 1
										else 0
									end
			from
				Staging.Fact_FixtureBerth fb
					outer apply	(
									select
										top 1
											e.ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents e with (nolock)
										where
											e.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
										order by
											e.StartDateTime
								) fe;
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstFixtureBerth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Find FirstPortBerth for transit time calculation
	begin try
		update
				fb with (tablock)
			set
				FirstPortBerth =	case
										when isnull(fpb.ParcelBerthAlternateKey, 0) = fb.ParcelBerthAlternateKey
											then 1
										else 0
									end
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
										distinct
											fbe.PostFixtureAlternateKey,
											first_value(fbe.ParcelBerthAlternateKey) over (partition by fbe.PostFixtureAlternateKey, fbe.LoadDischarge order by fbe.StartDateTime) ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventTypeId = 228
								) fpb
						on fpb.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and fpb.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstPortBerth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update transit time
	begin try
		update
				Staging.Fact_FixtureBerth
			set
				TransitTime =	case
									when isnull(HasMultipleDropAnchors, 0) = 1
										then POB_Berth - LastDropAnchor_POB
									else POB_Berth
								end
			where
				FirstPortBerth = 1
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstFixtureBerth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Flag WaitingTimeCandidate rows for transit time calculation.  Required: POB, NOR and Berth events
	begin try
		update
				fb with (tablock)
			set
				WaitingTimeCandidate = isnull(nor.HasNOR, 0) & isnull(pob.HasPOB, 0) & isnull(berth.HasBerth, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
										distinct
											fbe.PostFixtureAlternateKey,
											fbe.EventTypeId,
											fbe.LoadDischarge,
											1 HasNOR
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventTypeId = 219
								) nor
						on nor.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and nor.LoadDischarge = fb.LoadDischarge
					left join	(
									select
										distinct
											fbe.PostFixtureAlternateKey,
											fbe.EventTypeId,
											fbe.LoadDischarge,
											1 HasPOB
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventTypeId = 225
								) pob
						on pob.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and pob.LoadDischarge = fb.LoadDischarge
					left join	(
									select
										distinct
											fbe.PostFixtureAlternateKey,
											fbe.EventTypeId,
											fbe.LoadDischarge,
											1 HasBerth
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventTypeId = 228
								) berth
						on berth.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and berth.LoadDischarge = fb.LoadDischarge;
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitingTimeCandidate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationBerth_HoseOn
	begin try
		update
				fb with (tablock)
			set
				DurationBerth_HoseOn = isnull(fbed.DurationBerth_HoseOn, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationBerth_HoseOn,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 228	-- Berth/Alongside
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 257	-- Hose Connected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fb.PostFixtureAlternateKey = fbed.PostFixtureAlternateKey
						 and fb.ParcelBerthAlternateKey = fbed.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationHoseOn_CommenceLoad
	begin try
		update
				fb with (tablock)
			set
				DurationHoseOn_CommenceLoad = isnull(fbed.DurationHoseOn_CommenceLoad, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationHoseOn_CommenceLoad,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 257	-- Hose Connected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 262	-- Commence Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationHoseOn_CommenceLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationHoseOn_CommenceDischarge
	begin try
		update
				fb with (tablock)
			set
				DurationHoseOn_CommenceDischarge = isnull(fbed.DurationHoseOn_CommenceDischarge, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationHoseOn_CommenceDischarge,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 257	-- Hose Connected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 268	-- Commence Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationHoseOn_CommenceDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationBerth_HoseOff
	begin try
		update
				fb with (tablock)
			set
				DurationBerth_HoseOff = isnull(fbed.DurationBerth_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationBerth_HoseOff,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 228	-- Berth/Alongside
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 260	-- Hose Disconnected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationBerth_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationCompleteLoad_HoseOff
	begin try
		update
				fb with (tablock)
			set
				DurationCompleteLoad_HoseOff = isnull(fbed.DurationCompleteLoad_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationCompleteLoad_HoseOff,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 265	-- Completed Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 260	-- Hose Disconnected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationCompleteLoad_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationCompleteDischarge_HoseOff
	begin try
		update
				fb with (tablock)
			set
				DurationCompleteDischarge_HoseOff = isnull(fbed.DurationCompleteDischarge_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationCompleteDischarge_HoseOff,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 271	-- Complete Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 260	-- Hose Disconnected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationCommenceLoad_CompleteLoad
	begin try
		update
				fb with (tablock)
			set
				DurationCommenceLoad_CompleteLoad = isnull(fbed.DurationCommenceLoad_CompleteLoad, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationCommenceLoad_CompleteLoad,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 262	-- Commence Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 265	-- Completed Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationCommenceLoad_CompleteLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update DurationCommenceDischarge_CompleteDischarge
	begin try
		update
				fb with (tablock)
			set
				DurationCommenceDischarge_CompleteDischarge = isnull(fbed.DurationCommenceDischarge_CompleteDischarge, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationCommenceDischarge_CompleteDischarge,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 268	-- Commence Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 271	-- Complete Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating DurationCommenceDischarge_CompleteDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountNOR_Berth
	begin try
		update
				fb with (tablock)
			set
				TimeToCountNOR_Berth = isnull(fbed.TimeToCountNOR_Berth, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	TimeToCountNOR_Berth,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 219	-- NOR Tendered
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 228	-- Berth/Alongside
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountBerth_HoseOn
	begin try
		update
				fb with (tablock)
			set
				TimeToCountBerth_HoseOn = isnull(fbed.TimeToCountBerth_HoseOn, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	TimeToCountBerth_HoseOn,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 228	-- Berth/Alongside
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 257	-- Hose Connected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountHoseOn_CommenceLoad
	begin try
		update
				fb with (tablock)
			set
				TimeToCountHoseOn_CommenceLoad = isnull(fbed.TimeToCountHoseOn_CommenceLoad, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				TimeToCountHoseOn_CommenceLoad,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 257	-- Hose Connected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 262	-- Commence Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountHoseOn_CommenceLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountHoseOn_CommenceDischarge
	begin try
		update
				fb with (tablock)
			set
				TimeToCountHoseOn_CommenceDischarge = isnull(fbed.TimeToCountHoseOn_CommenceDischarge, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				TimeToCountHoseOn_CommenceDischarge,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 257	-- Hose Connected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 268	-- Commence Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountHoseOn_CommenceDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountBerth_HoseOff
	begin try
		update
				fb with (tablock)
			set
				TimeToCountBerth_HoseOff = isnull(fbed.TimeToCountBerth_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	TimeToCountBerth_HoseOff,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 228	-- Berth/Alongside
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 260	-- Hose Disconnected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountBerth_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountCompleteLoad_HoseOff
	begin try
		update
				fb with (tablock)
			set
				TimeToCountCompleteLoad_HoseOff = isnull(fbed.TimeToCountCompleteLoad_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	TimeToCountCompleteLoad_HoseOff,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 265	-- Completed Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 260	-- Hose Disconnected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountCompleteLoad_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountCompleteDischarge_HoseOff
	begin try
		update
				fb with (tablock)
			set
				TimeToCountCompleteDischarge_HoseOff = isnull(fbed.TimeToCountCompleteDischarge_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	TimeToCountCompleteDischarge_HoseOff,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 271	-- Complete Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 260	-- Hose Disconnected
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountCommenceLoad_CompleteLoad
	begin try
		update
				fb with (tablock)
			set
				TimeToCountCommenceLoad_CompleteLoad = isnull(fbed.TimeToCountCommenceLoad_CompleteLoad, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				TimeToCountCommenceLoad_CompleteLoad,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 262	-- Commence Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 265	-- Completed Loading
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountCommenceLoad_CompleteLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountCommenceDischarge_CompleteDischarge
	begin try
		update
				fb with (tablock)
			set
				TimeToCountCommenceDischarge_CompleteDischarge = isnull(fbed.TimeToCountCommenceDischarge_CompleteDischarge, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				TimeToCountCommenceDischarge_CompleteDischarge,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.EventNum >=	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 268	-- Commence Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)							
											and	fbe.EventNum <	(
																	select
																			min(EventNum)
																		from
																			Staging.Fact_FixtureBerthEvents en with (nolock)
																		where
																			en.EventTypeId = 271	-- Complete Discharge
																			and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
																			and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
																)
											and fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountCommenceDischarge_CompleteDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeNOR_Berth
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeNOR_Berth = isnull(DurationNOR_Berth, 0) - isnull(TimeToCountNOR_Berth, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeBerth_HoseOn
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeBerth_HoseOn = isnull(DurationBerth_HoseOn, 0) - isnull(TimeToCountBerth_HoseOn, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeHoseOn_CommenceLoad
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeHoseOn_CommenceLoad = isnull(DurationHoseOn_CommenceLoad, 0) - isnull(TimeToCountHoseOn_CommenceLoad, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeHoseOn_CommenceLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeHoseOn_CommenceDischarge
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeHoseOn_CommenceDischarge = isnull(DurationHoseOn_CommenceDischarge, 0) - isnull(TimeToCountHoseOn_CommenceDischarge, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeHoseOn_CommenceDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeBerth_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeBerth_HoseOff = isnull(DurationBerth_HoseOff, 0) - isnull(TimeToCountBerth_HoseOff, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeBerth_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeCompleteLoad_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeCompleteLoad_HoseOff = isnull(DurationCompleteLoad_HoseOff, 0) - isnull(TimeToCountCompleteLoad_HoseOff, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeCompleteLoad_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeCompleteDischarge_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeCompleteDischarge_HoseOff = isnull(DurationCompleteDischarge_HoseOff, 0) - isnull(TimeToCountCompleteDischarge_HoseOff, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeCommenceLoad_CompleteLoad
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeCommenceLoad_CompleteLoad = isnull(DurationCommenceLoad_CompleteLoad, 0) - isnull(TimeToCountCommenceLoad_CompleteLoad, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeCommenceLoad_CompleteLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update FreeTimeCommenceDischarge_CompleteDischarge
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				FreeTimeCommenceDischarge_CompleteDischarge = isnull(DurationCommenceDischarge_CompleteDischarge, 0) - isnull(TimeToCountCommenceDischarge_CompleteDischarge, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating FreeTimeCommenceDischarge_CompleteDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountPumpingTime
	begin try
		update
				fb with (tablock)
			set
				TimeToCountPumpingTime = isnull(fbed.TimeToCountPumpingTime, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	TimeToCountPumpingTime,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.IsLayTime = 'Y'
											and fbe.IsPumpingTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
						on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountPumpingTime - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update TimeToCountPumpingRate
	begin try
		update
				fbe with (tablock)
			set
				TimeToCountPumpingRate =	case										
												when isnull(fbed.TimeToCountPumpingTime, 0.0) > 0
													then isnull(fbe.ParcelQuantity/fbed.TimeToCountPumpingTime, 0)
												else 0
											end
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	TimeToCountPumpingTime,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe with (nolock)
						where
							fbe.IsLayTime = 'Y'
							and fbe.IsPumpingTime = 'Y'
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
					join Staging.Fact_FixtureBerth fbe
						on fbed.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
							and fbed.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
			where
				fbed.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating TimeToCountPumpingRate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaytimeUsed
	begin try
		update
				fb with (tablock)
			set
				LaytimeUsed = isnull(fbed.LaytimeUsed, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LaytimeUsed,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.IsLayTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
						on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating LaytimeUsed - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update PumpTime
	begin try
		update
				fb with (tablock)
			set
				PumpTime = isnull(fbed.PumpTime, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				PumpTime,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.IsPumpingTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
						on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating PumpTime - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update BerthPumpRate
	begin try
		update
				fb with (tablock)
			set
				BerthPumpRate =	case										
									when isnull(fbed.PumpTime, 0.0) > 0
										then isnull(fb.ParcelQuantity/fbed.PumpTime, 0)
									else 0
								end
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				PumpTime,
											fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
										from
											Staging.Fact_FixtureBerthEvents fbe with (nolock)
										where
											fbe.IsPumpingTime = 'Y'
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
						on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
							and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating BerthPumpRate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ParcelQuantityTShirtSize
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				ParcelQuantityTShirtSize =	case
												when fbe.ParcelQuantity between 0.0 and 2000.0
													then '0-2000'
												when fbe.ParcelQuantity between 2001.0 and 5000.0
													then '2001-5000'
												when fbe.ParcelQuantity between 5001.0 and 10000.0
													then '5001-10000'
												when fbe.ParcelQuantity > 10000.0
													then '10001+'
												else null
											end
			from
				Staging.Fact_FixtureBerth fbe
			where
				fbe.PostFixtureAlternateKey = PostFixtureAlternateKey
				and fbe.ParcelBerthAlternateKey = ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating ParcelQuantityTShirtSize - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update MinimumNORDate/MaxNORDate
	begin try
		update
				fb with (tablock)
			set
				MinimumNORDate = minnor.MinNORDate,
				MaximumNORDate = maxnor.MaxNORDate
			from
				Staging.Fact_FixtureBerth fb
					left join
						(
							select
									min(evt.StartDateTime) MinNORDate,
									evt.PostFixtureAlternateKey
								from
									Staging.Fact_FixtureBerthEvents evt with (nolock)
								where
									evt.EventTypeId = 219	-- NOR Tendered
									and evt.LoadDischarge = 'Load'
								group by
									evt.PostFixtureAlternateKey
						) minnor
							on minnor.PostFixtureAlternateKey = fb.PostFixtureAlternateKey				
					left join
						(
							select
									max(evt.StartDateTime) MaxNORDate,
									evt.PostFixtureAlternateKey
								from
									Staging.Fact_FixtureBerthEvents evt with (nolock)
								where
									evt.EventTypeId = 219	-- NOR Tendered
								group by
									evt.PostFixtureAlternateKey
						) maxnor
							on maxnor.PostFixtureAlternateKey = fb.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating MinimumNORDate/MaxNORDate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update VoyageDuration
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				VoyageDuration = datediff(day, fb.MinimumNORDate, fb.MaximumNORDate)
			from
				Staging.Fact_FixtureBerth fb;
	end try
	begin catch
		select @ErrorMsg = 'Updating VoyageDuration - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

		-- Update Laycan DateTime values
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LaycanCommencementDateTimeOriginal = convert(datetime, pf.LaycanCommencementOriginal) + convert(datetime, pf.Laycan_Commencement_Time_ADMIN),
				LaycanCommencementDateTimeNarrowed = convert(datetime, pf.LaycanCommencementNarrowed) + convert(datetime, pf.Laycan_Commencement_Time_ADMIN),
				LaycanCommencementDateTimeFinal = convert(datetime, pf.Laycan_Commencement_Final_QBC) + convert(datetime, pf.Laycan_Commencement_Time_ADMIN),
				LaycanCancellingDateTimeOriginal = convert(datetime, pf.LaycanCancelOrig) + convert(datetime, Laycan_Cancelling_Time_ADMIN),
				LaycanCancellingDateTimeNarrowed = convert(datetime, pf.LaycanCancellingNarrowed) + convert(datetime, Laycan_Cancelling_Time_ADMIN),
				LaycanCancellingDateTimeFinal = convert(datetime, pf.Laycan_Cancelling_Final_QBC) + convert(datetime, Laycan_Cancelling_Time_ADMIN)
			from
				PostFixtures pf with (nolock)
			where
				pf.QBRecId = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating Laycan DateTime values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	


	-- Update WithinLaycanOriginal
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WithinLaycanOriginal =	case
											when MinimumNORDate between LaycanCommencementDateTimeOriginal
													and LaycanCancellingDateTimeOriginal
												then 1
											else 0
										end;
	end try
	begin catch
		select @ErrorMsg = 'Updating WithinLaycanOriginal - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaycanOverUnderOriginal
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LaycanOverUnderOriginal =	case
												when MinimumNORDate <= LaycanCommencementDateTimeOriginal
													then datediff(hour, LaycanCommencementDateTimeOriginal, MinimumNORDate)/24.0
												when MinimumNORDate > LaycanCancellingDateTimeOriginal
													then datediff(hour, LaycanCancellingDateTimeOriginal, MinimumNORDate)/24.0
												else 0
											end;
	end try
	begin catch
		select @ErrorMsg = 'Updating LaycanOverUnderOriginal - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WithinLaycanNarrowed
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WithinLaycanNarrowed =	case
											when MinimumNORDate between LaycanCommencementDateTimeNarrowed
													and LaycanCancellingDateTimeNarrowed
												then 1
											else 0
										end;
	end try
	begin catch
		select @ErrorMsg = 'Updating WithinLaycanNarrowed - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaycanOverUnderNarrowed
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LaycanOverUnderNarrowed =	case
												when MinimumNORDate <= LaycanCommencementDateTimeNarrowed
													then datediff(hour, LaycanCommencementDateTimeNarrowed, MinimumNORDate)/24.0
												when MinimumNORDate > LaycanCancellingDateTimeNarrowed
													then datediff(hour, LaycanCancellingDateTimeNarrowed, MinimumNORDate)/24.0
												else 0
											end;
	end try
	begin catch
		select @ErrorMsg = 'Updating LaycanOverUnderNarrowed - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WithinLaycanFinal
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WithinLaycanFinal =	case
										when MinimumNORDate between LaycanCommencementDateTimeFinal
												and LaycanCancellingDateTimeFinal
											then 1
										else 0
									end;
	end try
	begin catch
		select @ErrorMsg = 'Updating WithinLaycanFinal - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaycanOverUnderFinal
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LaycanOverUnderFinal =	case
											when MinimumNORDate <= LaycanCommencementDateTimeFinal
												then datediff(hour, LaycanCommencementDateTimeFinal, MinimumNORDate)/24.0
											when MinimumNORDate > LaycanCancellingDateTimeFinal
												then datediff(hour, LaycanCancellingDateTimeFinal, MinimumNORDate)/24.0
											else 0
										end;
	end try
	begin catch
		select @ErrorMsg = 'Updating LaycanOverUnderFinal - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Find comparable, averable Free and lay times and aggregate them for comparison
	begin try
		with FixtureBerthEventTimeAggregations	(
													PortBerthKey,
													ProductType,
													ParcelQuantityTShirtSize,
													LoadDischarge,
													AverageFreeTimeNOR_Berth,
													AverageFreeTimeBerth_HoseOn,
													AverageFreeTimeHoseOn_CommenceLoad,
													AverageFreeTimeHoseOn_CommenceDischarge,
													AverageFreeTimeBerth_HoseOff,
													AverageFreeTimeCompleteLoad_HoseOff,
													AverageFreeTimeCompleteDischarge_HoseOff,
													AverageFreeTimeCommenceLoad_CompleteLoad,
													AverageFreeTimeCommenceDischarge_CompleteDischarge,
													AverageDurationNOR_Berth,
													AverageDurationBerth_HoseOn,
													AverageDurationHoseOn_CommenceLoad,
													AverageDurationHoseOn_CommenceDischarge,
													AverageDurationBerth_HoseOff,
													AverageDurationCompleteLoad_HoseOff,
													AverageDurationCompleteDischarge_HoseOff,
													AverageDurationCommenceLoad_CompleteLoad,
													AverageDurationCommenceDischarge_CompleteDischarge,
													AverageTimeToCountNOR_Berth,
													AverageTimeToCountBerth_HoseOn,
													AverageTimeToCountHoseOn_CommenceLoad,
													AverageTimeToCountHoseOn_CommenceDischarge,
													AverageTimeToCountBerth_HoseOff,
													AverageTimeToCountCompleteLoad_HoseOff,
													AverageTimeToCountCompleteDischarge_HoseOff,
													AverageTimeToCountCommenceLoad_CompleteLoad,
													AverageTimeToCountCommenceDischarge_CompleteDischarge,
													AverageLayTimePumpingTime,
													AverageLayTimePumpingRate,
													AverageLaytimeUsed,
													AverageLaytimeAllowed,
													AveragePumpTime
												)
		as
		(
			select
					fb.PortBerthKey,
					fb.ProductType,
					fb.ParcelQuantityTShirtSize,
					fb.LoadDischarge,
					avg(fb.FreeTimeNOR_Berth)										AverageFreeTimeNOR_Berth,
					avg(fb.FreeTimeBerth_HoseOn)									AverageFreeTimeBerth_HoseOn,
					avg(fb.FreeTimeHoseOn_CommenceLoad)								AverageFreeTimeHoseOn_CommenceLoad,
					avg(fb.FreeTimeHoseOn_CommenceDischarge)						AverageFreeTimeHoseOn_CommenceDischarge,
					avg(fb.FreeTimeBerth_HoseOff)									AverageFreeTimeBerth_HoseOff,
					avg(fb.FreeTimeCompleteLoad_HoseOff)							AverageFreeTimeCompleteLoad_HoseOff,
					avg(fb.FreeTimeCompleteDischarge_HoseOff)						AverageFreeTimeCompleteDischarge_HoseOff,
					avg(fb.FreeTimeCommenceLoad_CompleteLoad)						AverageFreeTimeCommenceLoad_CompleteLoad,
					avg(fb.FreeTimeCommenceDischarge_CompleteDischarge)				AverageFreeTimeCommenceDischarge_CompleteDischarge,
					avg(fb.DurationNOR_Berth)										AverageDurationNOR_Berth,
					avg(fb.DurationBerth_HoseOn)									AverageDurationBerth_HoseOn,
					avg(fb.DurationHoseOn_CommenceLoad)								AverageDurationHoseOn_CommenceLoad,
					avg(fb.DurationHoseOn_CommenceDischarge)						AverageDurationHoseOn_CommenceDischarge,
					avg(fb.DurationBerth_HoseOff)									AverageDurationBerth_HoseOff,
					avg(fb.DurationCompleteLoad_HoseOff)							AverageDurationCompleteLoad_HoseOff,
					avg(fb.DurationCompleteDischarge_HoseOff)						AverageDurationCompleteDischarge_HoseOff,
					avg(fb.DurationCommenceLoad_CompleteLoad)						AverageDurationCommenceLoad_CompleteLoad,
					avg(fb.DurationCommenceDischarge_CompleteDischarge)				AverageDurationCommenceDischarge_CompleteDischarge,
					avg(fb.TimeToCountNOR_Berth)									AverageTimeToCountNOR_Berth,
					avg(fb.TimeToCountBerth_HoseOn)									AverageTimeToCountBerth_HoseOn,
					avg(fb.TimeToCountHoseOn_CommenceLoad)							AverageTimeToCountHoseOn_CommenceLoad,
					avg(fb.TimeToCountHoseOn_CommenceDischarge)						AverageTimeToCountHoseOn_CommenceDischarge,
					avg(fb.TimeToCountBerth_HoseOff)								AverageTimeToCountBerth_HoseOff,
					avg(fb.TimeToCountCompleteLoad_HoseOff)							AverageTimeToCountCompleteLoad_HoseOff,
					avg(fb.TimeToCountCompleteDischarge_HoseOff)					AverageTimeToCountCompleteDischarge_HoseOff,
					avg(fb.TimeToCountCommenceLoad_CompleteLoad)					AverageTimeToCountCommenceLoad_CompleteLoad,
					avg(fb.TimeToCountCommenceDischarge_CompleteDischarge)			AverageTimeToCountCommenceDischarge_CompleteDischarge,
					avg(fb.TimeToCountPumpingTime)									AverageLayTimePumpingTime,
					avg	(
							case
								when fb.TimeToCountPumpingRate > 2500
									then null
								else fb.TimeToCountPumpingRate
							end
						)															AverageLayTimePumpingRate,
					avg(fb.LaytimeUsed)												AverageLaytimeUsed,
					avg(fb.LaytimeAllowed)											AverageLaytimeAllowed,
					avg(fb.PumpTime)												AveragePumpTime
				from
					Staging.Fact_FixtureBerth fb with (nolock)
				where
					fb.PortBerthKey is not null
					and fb.ProductType is not null
					and fb.ParcelQuantityTShirtSize is not null
					and fb.LoadDischarge is not null
					and fb.FirstEventDateKey >= 20190101
				group by
					fb.PortBerthKey,
					fb.ProductType,
					fb.ParcelQuantityTShirtSize,
					fb.LoadDischarge
		)
		
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				AverageFreeTimeNOR_Berth = fba.AverageFreeTimeNOR_Berth,
				AverageFreeTimeBerth_HoseOn = fba.AverageFreeTimeBerth_HoseOn,
				AverageFreeTimeHoseOn_CommenceLoad = fba.AverageFreeTimeHoseOn_CommenceLoad,
				AverageFreeTimeHoseOn_CommenceDischarge = fba.AverageFreeTimeHoseOn_CommenceDischarge,
				AverageFreeTimeBerth_HoseOff = fba.AverageFreeTimeBerth_HoseOff,
				AverageFreeTimeCompleteLoad_HoseOff = fba.AverageFreeTimeCompleteLoad_HoseOff,
				AverageFreeTimeCompleteDischarge_HoseOff = fba.AverageFreeTimeCompleteDischarge_HoseOff,
				AverageFreeTimeCommenceLoad_CompleteLoad = fba.AverageFreeTimeCommenceLoad_CompleteLoad,
				AverageFreeTimeCommenceDischarge_CompleteDischarge = fba.AverageFreeTimeCommenceDischarge_CompleteDischarge,
				AverageDurationNOR_Berth = fba.AverageDurationNOR_Berth,
				AverageDurationBerth_HoseOn = fba.AverageDurationBerth_HoseOn,
				AverageDurationHoseOn_CommenceLoad = fba.AverageDurationHoseOn_CommenceLoad,
				AverageDurationHoseOn_CommenceDischarge = fba.AverageDurationHoseOn_CommenceDischarge,
				AverageDurationBerth_HoseOff = fba.AverageDurationBerth_HoseOff,
				AverageDurationCompleteLoad_HoseOff = fba.AverageDurationCompleteLoad_HoseOff,
				AverageDurationCompleteDischarge_HoseOff = fba.AverageDurationCompleteDischarge_HoseOff,
				AverageDurationCommenceLoad_CompleteLoad = fba.AverageDurationCommenceLoad_CompleteLoad,
				AverageDurationCommenceDischarge_CompleteDischarge = fba.AverageDurationCommenceDischarge_CompleteDischarge,
				AverageTimeToCountNOR_Berth = fba.AverageTimeToCountNOR_Berth,
				AverageTimeToCountBerth_HoseOn = fba.AverageTimeToCountBerth_HoseOn,
				AverageTimeToCountHoseOn_CommenceLoad = fba.AverageTimeToCountHoseOn_CommenceLoad,
				AverageTimeToCountHoseOn_CommenceDischarge = fba.AverageTimeToCountHoseOn_CommenceDischarge,
				AverageTimeToCountBerth_HoseOff = fba.AverageTimeToCountBerth_HoseOff,
				AverageTimeToCountCompleteLoad_HoseOff = fba.AverageTimeToCountCompleteLoad_HoseOff,
				AverageTimeToCountCompleteDischarge_HoseOff = fba.AverageTimeToCountCompleteDischarge_HoseOff,
				AverageTimeToCountCommenceLoad_CompleteLoad = fba.AverageTimeToCountCommenceLoad_CompleteLoad,
				AverageTimeToCountCommenceDischarge_CompleteDischarge = fba.AverageTimeToCountCommenceDischarge_CompleteDischarge,
				AverageLayTimePumpingTime = fba.AverageLayTimePumpingTime,
				AverageLayTimePumpingRate = fba.AverageLayTimePumpingRate,
				AverageLaytimeUsed = fba.AverageLaytimeUsed,
				AverageLaytimeAllowed = fba.AverageLaytimeAllowed,
				AveragePumpTime = fba.AveragePumpTime
			from
				FixtureBerthEventTimeAggregations fba
			where
				Staging.Fact_FixtureBerth.PortBerthKey = fba.PortBerthKey
				and Staging.Fact_FixtureBerth.ProductType = fba.ProductType
				and Staging.Fact_FixtureBerth.ParcelQuantityTShirtSize = fba.ParcelQuantityTShirtSize
				and Staging.Fact_FixtureBerth.LoadDischarge = fba.LoadDischarge;
				
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Truncate Warehouse table before insert to capture changes
	if object_id(N'Warehouse.Fact_FixtureBerth', 'U') is not null
		truncate table Warehouse.Fact_FixtureBerth;

	-- Insert new events into Warehouse table
	begin try
		insert
				Warehouse.Fact_FixtureBerth with (tablock)
			select
					sfb.PostFixtureAlternateKey,
					sfb.PortAlternateKey,
					sfb.BerthAlternateKey,
					sfb.LoadDischargeAlternateKey,
					sfb.ParcelBerthAlternateKey,
					sfb.LoadPortBerthKey,
					sfb.DischargePortBerthKey,
					sfb.PortBerthKey,
					sfb.PostFixtureKey,
					sfb.VesselKey,
					sfb.FirstEventDateKey,
					sfb.LoadPortKey,
					sfb.LoadBerthKey,
					sfb.DischargePortKey,
					sfb.ChartererKey,
					sfb.OwnerKey,
					sfb.ProductKey,
					sfb.ProductFixtureBerthQuantityKey,
					sfb.CPDateKey,
					sfb.COAKey,
					sfb.LoadDischarge,
					sfb.ProductType,
					sfb.ParcelQuantityTShirtSize,
					sfb.FreeTimeNOR_Berth,
					sfb.AverageFreeTimeNOR_Berth,
					sfb.FreeTimeBerth_HoseOn,
					sfb.AverageFreeTimeBerth_HoseOn,
					sfb.FreeTimeHoseOn_CommenceLoad,
					sfb.AverageFreeTimeHoseOn_CommenceLoad,
					sfb.FreeTimeHoseOn_CommenceDischarge,
					sfb.AverageFreeTimeHoseOn_CommenceDischarge,
					sfb.FreeTimeBerth_HoseOff,
					sfb.AverageFreeTimeBerth_HoseOff,
					sfb.FreeTimeCompleteLoad_HoseOff,
					sfb.AverageFreeTimeCompleteLoad_HoseOff,
					sfb.FreeTimeCompleteDischarge_HoseOff,
					sfb.AverageFreeTimeCompleteDischarge_HoseOff,
					sfb.FreeTimeCommenceLoad_CompleteLoad,
					sfb.AverageFreeTimeCommenceLoad_CompleteLoad,
					sfb.FreeTimeCommenceDischarge_CompleteDischarge,
					sfb.AverageFreeTimeCommenceDischarge_CompleteDischarge,
					sfb.DurationNOR_Berth,
					sfb.AverageDurationNOR_Berth,
					sfb.DurationBerth_HoseOn,
					sfb.AverageDurationBerth_HoseOn,
					sfb.DurationHoseOn_CommenceLoad,
					sfb.AverageDurationHoseOn_CommenceLoad,
					sfb.DurationHoseOn_CommenceDischarge,
					sfb.AverageDurationHoseOn_CommenceDischarge,
					sfb.DurationBerth_HoseOff,
					sfb.AverageDurationBerth_HoseOff,
					sfb.DurationCompleteLoad_HoseOff,
					sfb.AverageDurationCompleteLoad_HoseOff,
					sfb.DurationCompleteDischarge_HoseOff,
					sfb.AverageDurationCompleteDischarge_HoseOff,
					sfb.DurationCommenceLoad_CompleteLoad,
					sfb.AverageDurationCommenceLoad_CompleteLoad,
					sfb.DurationCommenceDischarge_CompleteDischarge,
					sfb.AverageDurationCommenceDischarge_CompleteDischarge,
					sfb.TimeToCountNOR_Berth,
					sfb.AverageTimeToCountNOR_Berth,
					sfb.TimeToCountBerth_HoseOn,
					sfb.AverageTimeToCountBerth_HoseOn,
					sfb.TimeToCountHoseOn_CommenceLoad,
					sfb.AverageTimeToCountHoseOn_CommenceLoad,
					sfb.TimeToCountHoseOn_CommenceDischarge,
					sfb.AverageTimeToCountHoseOn_CommenceDischarge,
					sfb.TimeToCountBerth_HoseOff,
					sfb.AverageTimeToCountBerth_HoseOff,
					sfb.TimeToCountCompleteLoad_HoseOff,
					sfb.AverageTimeToCountCompleteLoad_HoseOff,
					sfb.TimeToCountCompleteDischarge_HoseOff,
					sfb.AverageTimeToCountCompleteDischarge_HoseOff,
					sfb.TimeToCountCommenceLoad_CompleteLoad,
					sfb.AverageTimeToCountCommenceLoad_CompleteLoad,
					sfb.TimeToCountCommenceDischarge_CompleteDischarge,
					sfb.AverageTimeToCountCommenceDischarge_CompleteDischarge,
					sfb.TimeToCountPumpingTime,
					sfb.AverageLayTimePumpingTime,
					sfb.TimeToCountPumpingRate,
					sfb.AverageLayTimePumpingRate,
					sfb.ParcelQuantity,
					sfb.LaytimeUsed,
					sfb.AverageLaytimeUsed,
					sfb.LaytimeAllowed,
					sfb.AverageLaytimeAllowed,
					sfb.PumpTime,
					sfb.BerthPumpRate,
					sfb.AveragePumpTime,
					sfb.WithinLaycanOriginal,
					sfb.LaycanOverUnderOriginal,
					sfb.WithinLaycanFinal,
					sfb.LaycanOverUnderFinal,
					sfb.WithinLaycanNarrowed,
					sfb.LaycanOverUnderNarrowed,
					sfb.VoyageDuration,
					sfb.TransitTime,
					sfb.FirstFixtureBerth,
					sfb.FirstPortBerth,
					sfb.WaitingTimeCandidate,
					getdate() RowStartDate,
					getdate() RowUpdatedDate
				from
					Staging.Fact_FixtureBerth sfb with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end
go