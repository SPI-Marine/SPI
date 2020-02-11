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
Brian Boswick	06/26/2019	Added LaytimeCommenceLoad_CompleteLoad, LaytimeCommenceDischarge_CompleteDischarge,
							AverageLaytimeCommenceLoad_CompleteLoad and 
							AverageLaytimeCommenceDischarge_CompleteDischarge metrics
Brian Boswick	02/06/2020	Added ChartererKey and OwnerKey ETL logic
Brian Boswick	02/10/2020	Added ProductKey ETL logic
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
						join ParcelPorts loadport with (nolock)
							on p.RelatedLoadPortID = loadport.QBRecId
						join ParcelBerths loadberth with (nolock)
							on p.RelatedLoadBerth = loadberth.QBRecId
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
						join ParcelPorts dischport with (nolock)
							on p.RelatedDischPortId = dischport.QBRecId
						join ParcelBerths dischberth with (nolock)
							on p.RelatedDischBerth = dischberth.QBRecId
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
												PortBerthKey,
												PostFixtureKey,
												VesselKey,
												FirstEventDateKey,
												LoadPortKey,
												DischargePortKey,
												ChartererKey,
												OwnerKey,
												ProductKey,
												LoadDischarge,
												ParcelQuantity,
												LaytimeAllowed
											)
			select
				distinct
					ufb.PostFixtureAlternateKey					PostFixtureAlternateKey,
					isnull(ufb.PortAlternateKey, -1)			PortAlternateKey,
					isnull(ufb.BerthAlternateKey, -1)			BerthAlternateKey,
					ufb.LoadDischargeAlternateKey				LoadDischargeAlternateKey,
					isnull(ufb.ParcelBerthAlternateKey, -1)		ParcelBerthAlternateKey,
					isnull(portberth.PortBerthKey, -1)			PortBerthKey,
					isnull(wpostfixture.PostFixtureKey, -1)		PostFixtureKey,
					isnull(vessel.VesselKey, -1)				VesselKey,
					-1											FirstEventDateKey,
					isnull(wloadport.PortKey, -1)				LoadPortKey,
					isnull(wdischport.PortKey, -1)				DischargePortKey,
					isnull(wch.ChartererKey, -1)				ChartererKey,
					isnull(wo.OwnerKey, -1)						OwnerKey,
					-1											ProductKey,
					ufb.LoadDischarge							LoadDischarge,
					ufb.ParcelQuantity							ParcelQuantity,
					ufb.LaytimeAllowed							LaytimeAllowed
				from
					AggregatedParcelQuantity ufb
						left join Warehouse.Dim_PostFixture wpostfixture with (nolock)
							on wpostfixture.PostFixtureAlternateKey = ufb.PostFixtureAlternateKey
						left join PostFixtures epostfixture with (nolock)
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Vessel vessel with (nolock)
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join ParcelPorts pp
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
						left join FullStyles fs with (nolock)
							on epostfixture.RelatedChartererFullStyle = fs.QBRecId
						left join Warehouse.Dim_Owner wo with (nolock)
							on wo.OwnerAlternateKey = fs.RelatedOwnerParentId
						left join Warehouse.Dim_Charterer wch with (nolock)
							on wch.ChartererAlternateKey = fs.RelatedChartererParentID	end try
	begin catch
		select @ErrorMsg = 'Staging FixtureBerth records - ' + error_message();
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
							min(convert(date, e.StartDate)) FirstEventDate,
							pb.RelatedSpiFixtureId PostFixtureAlternateKey
						from
							SOFEvents e with (nolock)
								join ParcelBerths pb  with (nolock)
									on e.RelatedParcelBerthId = pb.QBRecId
						group by
							pb.RelatedSpiFixtureId
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
					e.RelatedParcelBerthId,
					e.RelatedPortTimeEventId,
					wevent.EventType,
					wevent.Duration,
					e.LtUsedProrationAmtHrs_QBC,
					try_convert(datetime, wevent.StartDateTime, 103) StartDateTime,
					wevent.LoadDischarge,
					wevent.IsPumpingTime,
					wevent.IsLaytime
				from
					Warehouse.Fact_SOFEvent wevent with (nolock)
						join Warehouse.Dim_PostFixture wpf with (nolock)
							on wpf.PostFixtureKey = wevent.PostFixtureKey
						join SOFEvents e with (nolock)
							on wevent.EventAlternateKey = e.QBRecId
				where
					wevent.PostFixtureKey > 0
					and isnull(wevent.Duration, 0) >= 0
		),
		OrderedEvents	(
							PostFixtureAlternateKey,
							ParcelBerthAlternateKey,
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
					row_number() over	(
											partition by ed.ParcelBerthAlternateKey
											order by ed.StartDateTime
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

	-- Update DurationNOR_Berth
	begin try
		update
				fb with (tablock)
			set
				DurationNOR_Berth = isnull(fbed.DurationNOR_Berth, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				DurationNOR_Berth,
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
										group by
											fbe.PostFixtureAlternateKey,
											fbe.ParcelBerthAlternateKey
								) fbed
					on fbed.PostFixtureAlternateKey = fb.PostFixtureAlternateKey
						and fbed.ParcelBerthAlternateKey = fb.ParcelBerthAlternateKey;

	end try
	begin catch
		select @ErrorMsg = 'Updating DurationNOR_Berth - ' + error_message();
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

	-- Update LayTimeNOR_Berth
	begin try
		update
				fb with (tablock)
			set
				LayTimeNOR_Berth = isnull(fbed.LayTimeNOR_Berth, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LayTimeNOR_Berth,
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
		select @ErrorMsg = 'Updating LayTimeNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeBerth_HoseOn
	begin try
		update
				fb with (tablock)
			set
				LayTimeBerth_HoseOn = isnull(fbed.LayTimeBerth_HoseOn, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LayTimeBerth_HoseOn,
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
		select @ErrorMsg = 'Updating LayTimeBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeHoseOn_CommenceLoad
	begin try
		update
				fb with (tablock)
			set
				LayTimeHoseOn_CommenceLoad = isnull(fbed.LayTimeHoseOn_CommenceLoad, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				LayTimeHoseOn_CommenceLoad,
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
		select @ErrorMsg = 'Updating LayTimeHoseOn_CommenceLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeHoseOn_CommenceDischarge
	begin try
		update
				fb with (tablock)
			set
				LayTimeHoseOn_CommenceDischarge = isnull(fbed.LayTimeHoseOn_CommenceDischarge, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				LayTimeHoseOn_CommenceDischarge,
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
		select @ErrorMsg = 'Updating LayTimeHoseOn_CommenceDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeBerth_HoseOff
	begin try
		update
				fb with (tablock)
			set
				LayTimeBerth_HoseOff = isnull(fbed.LayTimeBerth_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LayTimeBerth_HoseOff,
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
		select @ErrorMsg = 'Updating LayTimeBerth_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeCompleteLoad_HoseOff
	begin try
		update
				fb with (tablock)
			set
				LayTimeCompleteLoad_HoseOff = isnull(fbed.LayTimeCompleteLoad_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LayTimeCompleteLoad_HoseOff,
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
		select @ErrorMsg = 'Updating LayTimeCompleteLoad_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeCompleteDischarge_HoseOff
	begin try
		update
				fb with (tablock)
			set
				LayTimeCompleteDischarge_HoseOff = isnull(fbed.LayTimeCompleteDischarge_HoseOff, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LayTimeCompleteDischarge_HoseOff,
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
		select @ErrorMsg = 'Updating LayTimeCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaytimeCommenceLoad_CompleteLoad
	begin try
		update
				fb with (tablock)
			set
				LaytimeCommenceLoad_CompleteLoad = isnull(fbed.LaytimeCommenceLoad_CompleteLoad, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				LaytimeCommenceLoad_CompleteLoad,
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
		select @ErrorMsg = 'Updating LaytimeCommenceLoad_CompleteLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaytimeCommenceDischarge_CompleteDischarge
	begin try
		update
				fb with (tablock)
			set
				LaytimeCommenceDischarge_CompleteDischarge = isnull(fbed.LaytimeCommenceDischarge_CompleteDischarge, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.Duration)				LaytimeCommenceDischarge_CompleteDischarge,
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
		select @ErrorMsg = 'Updating LaytimeCommenceDischarge_CompleteDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeNOR_Berth
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeNOR_Berth = isnull(DurationNOR_Berth, 0) - isnull(LayTimeNOR_Berth, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeBerth_HoseOn
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeBerth_HoseOn = isnull(DurationBerth_HoseOn, 0) - isnull(LayTimeBerth_HoseOn, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeHoseOn_CommenceLoad
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeHoseOn_CommenceLoad = isnull(DurationHoseOn_CommenceLoad, 0) - isnull(LayTimeHoseOn_CommenceLoad, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeHoseOn_CommenceLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeHoseOn_CommenceDischarge
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeHoseOn_CommenceDischarge = isnull(DurationHoseOn_CommenceDischarge, 0) - isnull(LayTimeHoseOn_CommenceDischarge, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeHoseOn_CommenceDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeBerth_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeBerth_HoseOff = isnull(DurationBerth_HoseOff, 0) - isnull(LayTimeBerth_HoseOff, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeBerth_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeCompleteLoad_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeCompleteLoad_HoseOff = isnull(DurationCompleteLoad_HoseOff, 0) - isnull(LayTimeCompleteLoad_HoseOff, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeCompleteLoad_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeCompleteDischarge_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeCompleteDischarge_HoseOff = isnull(DurationCompleteDischarge_HoseOff, 0) - isnull(LayTimeCompleteDischarge_HoseOff, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeCommenceLoad_CompleteLoad
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeCommenceLoad_CompleteLoad = isnull(DurationCommenceLoad_CompleteLoad, 0) - isnull(LaytimeCommenceLoad_CompleteLoad, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeCommenceLoad_CompleteLoad - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeCommenceDischarge_CompleteDischarge
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeCommenceDischarge_CompleteDischarge = isnull(DurationCommenceDischarge_CompleteDischarge, 0) - isnull(LaytimeCommenceDischarge_CompleteDischarge, 0)
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeCommenceDischarge_CompleteDischarge - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimePumpingTime
	begin try
		update
				fb with (tablock)
			set
				LayTimePumpingTime = isnull(fbed.LayTimePumpingTime, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LayTimePumpingTime,
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
		select @ErrorMsg = 'Updating LayTimePumpingTime - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimePumpingRate
	begin try
		update
				fbe with (tablock)
			set
				LayTimePumpingRate =	case										
											when isnull(fbed.LayTimePumpingTime, 0.0) > 0
												then isnull(fbe.ParcelQuantity/fbed.LayTimePumpingTime, 0)
											else 0
										end
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimePumpingTime,
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
		select @ErrorMsg = 'Updating LayTimePumpingRate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaytimeActual
	begin try
		update
				fb with (tablock)
			set
				LaytimeActual = isnull(fbed.LaytimeActual, 0)
			from
				Staging.Fact_FixtureBerth fb
					left join	(
									select
											sum(fbe.LayTimeUsedProrated)	LaytimeActual,
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
		select @ErrorMsg = 'Updating LaytimeActual - ' + error_message();
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

	-- Update WithinLaycanOriginal
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WithinLaycanOriginal =	case
											when MinimumNORDate between convert(date, pf.LaycanCommencementOriginal)
													and convert(date, pf.LaycanCancelOrig)
												then 1
											else 0
										end
			from
				PostFixtures pf with (nolock)
			where
				pf.QBRecId = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
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
												when MinimumNORDate <= convert(date, pf.LaycanCommencementOriginal)
													then datediff(day, convert(date, pf.LaycanCommencementOriginal), MinimumNORDate)
												when MinimumNORDate > convert(date, pf.LaycanCancelOrig)
													then datediff(day, convert(date, pf.LaycanCancelOrig), MinimumNORDate)
												else 0
											end
			from
				PostFixtures pf with (nolock)
			where
				pf.QBRecId = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating LaycanOverUnderOriginal - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WithinLaycanFinal
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WithinLaycanFinal =	case
										when MinimumNORDate between convert(date, pf.Laycan_Commencement_Final_QBC)
												and convert(date, pf.Laycan_Cancelling_Final_QBC)
											then 1
										else 0
									end
			from
				PostFixtures pf with (nolock)
			where
				pf.QBRecId = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
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
											when MinimumNORDate <= convert(date, pf.Laycan_Commencement_Final_QBC)
												then datediff(day, convert(date, pf.Laycan_Commencement_Final_QBC), MinimumNORDate)
											when MinimumNORDate > convert(date, pf.Laycan_Cancelling_Final_QBC)
												then datediff(day, convert(date, pf.Laycan_Cancelling_Final_QBC), MinimumNORDate)
											else 0
										end
			from
				PostFixtures pf with (nolock)
			where
				pf.QBRecId = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating LaycanOverUnderFinal - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Find comparable, averable wait and lay times and aggregate them for comparison
	begin try
		with FixtureBerthEventTimeAggregations	(
													PortBerthKey,
													ProductType,
													ParcelQuantityTShirtSize,
													LoadDischarge,
													AverageWaitTimeNOR_Berth,
													AverageWaitTimeBerth_HoseOn,
													AverageWaitTimeHoseOn_CommenceLoad,
													AverageWaitTimeHoseOn_CommenceDischarge,
													AverageWaitTimeBerth_HoseOff,
													AverageWaitTimeCompleteLoad_HoseOff,
													AverageWaitTimeCompleteDischarge_HoseOff,
													AverageWaitTimeCommenceLoad_CompleteLoad,
													AverageWaitTimeCommenceDischarge_CompleteDischarge,
													AverageDurationNOR_Berth,
													AverageDurationBerth_HoseOn,
													AverageDurationHoseOn_CommenceLoad,
													AverageDurationHoseOn_CommenceDischarge,
													AverageDurationBerth_HoseOff,
													AverageDurationCompleteLoad_HoseOff,
													AverageDurationCompleteDischarge_HoseOff,
													AverageDurationCommenceLoad_CompleteLoad,
													AverageDurationCommenceDischarge_CompleteDischarge,
													AverageLayTimeNOR_Berth,
													AverageLayTimeBerth_HoseOn,
													AverageLayTimeHoseOn_CommenceLoad,
													AverageLayTimeHoseOn_CommenceDischarge,
													AverageLayTimeBerth_HoseOff,
													AverageLayTimeCompleteLoad_HoseOff,
													AverageLayTimeCompleteDischarge_HoseOff,
													AverageLaytimeCommenceLoad_CompleteLoad,
													AverageLaytimeCommenceDischarge_CompleteDischarge,
													AverageLayTimePumpingTime,
													AverageLayTimePumpingRate,
													AverageLaytimeActual,
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
					avg(fb.WaitTimeNOR_Berth)										AverageWaitTimeNOR_Berth,
					avg(fb.WaitTimeBerth_HoseOn)									AverageWaitTimeBerth_HoseOn,
					avg(fb.WaitTimeHoseOn_CommenceLoad)								AverageWaitTimeHoseOn_CommenceLoad,
					avg(fb.WaitTimeHoseOn_CommenceDischarge)						AverageWaitTimeHoseOn_CommenceDischarge,
					avg(fb.WaitTimeBerth_HoseOff)									AverageWaitTimeBerth_HoseOff,
					avg(fb.WaitTimeCompleteLoad_HoseOff)							AverageWaitTimeCompleteLoad_HoseOff,
					avg(fb.WaitTimeCompleteDischarge_HoseOff)						AverageWaitTimeCompleteDischarge_HoseOff,
					avg(fb.WaitTimeCommenceLoad_CompleteLoad)						AverageWaitTimeCommenceLoad_CompleteLoad,
					avg(fb.WaitTimeCommenceDischarge_CompleteDischarge)				AverageWaitTimeCommenceDischarge_CompleteDischarge,
					avg(fb.DurationNOR_Berth)										AverageDurationNOR_Berth,
					avg(fb.DurationBerth_HoseOn)									AverageDurationBerth_HoseOn,
					avg(fb.DurationHoseOn_CommenceLoad)								AverageDurationHoseOn_CommenceLoad,
					avg(fb.DurationHoseOn_CommenceDischarge)						AverageDurationHoseOn_CommenceDischarge,
					avg(fb.DurationBerth_HoseOff)									AverageDurationBerth_HoseOff,
					avg(fb.DurationCompleteLoad_HoseOff)							AverageDurationCompleteLoad_HoseOff,
					avg(fb.DurationCompleteDischarge_HoseOff)						AverageDurationCompleteDischarge_HoseOff,
					avg(fb.DurationCommenceLoad_CompleteLoad)						AverageDurationCommenceLoad_CompleteLoad,
					avg(fb.DurationCommenceDischarge_CompleteDischarge)				AverageDurationCommenceDischarge_CompleteDischarge,
					avg(fb.LayTimeNOR_Berth)										AverageLayTimeNOR_Berth,
					avg(fb.LayTimeBerth_HoseOn)										AverageLayTimeBerth_HoseOn,
					avg(fb.LayTimeHoseOn_CommenceLoad)								AverageLayTimeHoseOn_CommenceLoad,
					avg(fb.LayTimeHoseOn_CommenceDischarge)							AverageLayTimeHoseOn_CommenceDischarge,
					avg(fb.LayTimeBerth_HoseOff)									AverageLayTimeBerth_HoseOff,
					avg(fb.LayTimeCompleteLoad_HoseOff)								AverageLayTimeCompleteLoad_HoseOff,
					avg(fb.LayTimeCompleteDischarge_HoseOff)						AverageLayTimeCompleteDischarge_HoseOff,
					avg(fb.LaytimeCommenceLoad_CompleteLoad)						AverageLaytimeCommenceLoad_CompleteLoad,
					avg(fb.LaytimeCommenceDischarge_CompleteDischarge)				AverageLaytimeCommenceDischarge_CompleteDischarge,
					avg(fb.LayTimePumpingTime)										AverageLayTimePumpingTime,
					avg(fb.LayTimePumpingRate)										AverageLayTimePumpingRate,
					avg(fb.LaytimeActual)											AverageLaytimeActual,
					avg(fb.LaytimeAllowed)											AverageLaytimeAllowed,
					avg(fb.PumpTime)												AveragePumpTime
				from
					Staging.Fact_FixtureBerth fb with (nolock)
				where
					PortBerthKey = fb.PortBerthKey
					and ProductType = fb.ProductType
					and ParcelQuantityTShirtSize = fb.ParcelQuantityTShirtSize
					and LoadDischarge = fb.LoadDischarge
				group by
					fb.PortBerthKey,
					fb.ProductType,
					fb.ParcelQuantityTShirtSize,
					fb.LoadDischarge
		)
		
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				AverageWaitTimeNOR_Berth = fba.AverageWaitTimeNOR_Berth,
				AverageWaitTimeBerth_HoseOn = fba.AverageWaitTimeBerth_HoseOn,
				AverageWaitTimeHoseOn_CommenceLoad = fba.AverageWaitTimeHoseOn_CommenceLoad,
				AverageWaitTimeHoseOn_CommenceDischarge = fba.AverageWaitTimeHoseOn_CommenceDischarge,
				AverageWaitTimeBerth_HoseOff = fba.AverageWaitTimeBerth_HoseOff,
				AverageWaitTimeCompleteLoad_HoseOff = fba.AverageWaitTimeCompleteLoad_HoseOff,
				AverageWaitTimeCompleteDischarge_HoseOff = fba.AverageWaitTimeCompleteDischarge_HoseOff,
				AverageWaitTimeCommenceLoad_CompleteLoad = fba.AverageWaitTimeCommenceLoad_CompleteLoad,
				AverageWaitTimeCommenceDischarge_CompleteDischarge = fba.AverageWaitTimeCommenceDischarge_CompleteDischarge,
				AverageDurationNOR_Berth = fba.AverageDurationNOR_Berth,
				AverageDurationBerth_HoseOn = fba.AverageDurationBerth_HoseOn,
				AverageDurationHoseOn_CommenceLoad = fba.AverageDurationHoseOn_CommenceLoad,
				AverageDurationHoseOn_CommenceDischarge = fba.AverageDurationHoseOn_CommenceDischarge,
				AverageDurationBerth_HoseOff = fba.AverageDurationBerth_HoseOff,
				AverageDurationCompleteLoad_HoseOff = fba.AverageDurationCompleteLoad_HoseOff,
				AverageDurationCompleteDischarge_HoseOff = fba.AverageDurationCompleteDischarge_HoseOff,
				AverageDurationCommenceLoad_CompleteLoad = fba.AverageDurationCommenceLoad_CompleteLoad,
				AverageDurationCommenceDischarge_CompleteDischarge = fba.AverageDurationCommenceDischarge_CompleteDischarge,
				AverageLayTimeNOR_Berth = fba.AverageLayTimeNOR_Berth,
				AverageLayTimeBerth_HoseOn = fba.AverageLayTimeBerth_HoseOn,
				AverageLayTimeHoseOn_CommenceLoad = fba.AverageLayTimeHoseOn_CommenceLoad,
				AverageLayTimeHoseOn_CommenceDischarge = fba.AverageLayTimeHoseOn_CommenceDischarge,
				AverageLayTimeBerth_HoseOff = fba.AverageLayTimeBerth_HoseOff,
				AverageLayTimeCompleteLoad_HoseOff = fba.AverageLayTimeCompleteLoad_HoseOff,
				AverageLayTimeCompleteDischarge_HoseOff = fba.AverageLayTimeCompleteDischarge_HoseOff,
				AverageLaytimeCommenceLoad_CompleteLoad = fba.AverageLaytimeCommenceLoad_CompleteLoad,
				AverageLaytimeCommenceDischarge_CompleteDischarge = fba.AverageLaytimeCommenceDischarge_CompleteDischarge,
				AverageLayTimePumpingTime = fba.AverageLayTimePumpingTime,
				AverageLayTimePumpingRate = fba.AverageLayTimePumpingRate,
				AverageLaytimeActual = fba.AverageLaytimeActual,
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
					sfb.PortBerthKey,
					sfb.PostFixtureKey,
					sfb.VesselKey,
					sfb.FirstEventDateKey,
					sfb.LoadPortKey,
					sfb.DischargePortKey,
					sfb.ChartererKey,
					sfb.OwnerKey,
					sfb.ProductKey,
					sfb.LoadDischarge,
					sfb.ProductType,
					sfb.ParcelQuantityTShirtSize,
					sfb.WaitTimeNOR_Berth,
					sfb.AverageWaitTimeNOR_Berth,
					sfb.WaitTimeBerth_HoseOn,
					sfb.AverageWaitTimeBerth_HoseOn,
					sfb.WaitTimeHoseOn_CommenceLoad,
					sfb.AverageWaitTimeHoseOn_CommenceLoad,
					sfb.WaitTimeHoseOn_CommenceDischarge,
					sfb.AverageWaitTimeHoseOn_CommenceDischarge,
					sfb.WaitTimeBerth_HoseOff,
					sfb.AverageWaitTimeBerth_HoseOff,
					sfb.WaitTimeCompleteLoad_HoseOff,
					sfb.AverageWaitTimeCompleteLoad_HoseOff,
					sfb.WaitTimeCompleteDischarge_HoseOff,
					sfb.AverageWaitTimeCompleteDischarge_HoseOff,
					sfb.WaitTimeCommenceLoad_CompleteLoad,
					sfb.AverageWaitTimeCommenceLoad_CompleteLoad,
					sfb.WaitTimeCommenceDischarge_CompleteDischarge,
					sfb.AverageWaitTimeCommenceDischarge_CompleteDischarge,
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
					sfb.LayTimeNOR_Berth,
					sfb.AverageLayTimeNOR_Berth,
					sfb.LayTimeBerth_HoseOn,
					sfb.AverageLayTimeBerth_HoseOn,
					sfb.LayTimeHoseOn_CommenceLoad,
					sfb.AverageLayTimeHoseOn_CommenceLoad,
					sfb.LayTimeHoseOn_CommenceDischarge,
					sfb.AverageLayTimeHoseOn_CommenceDischarge,
					sfb.LayTimeBerth_HoseOff,
					sfb.AverageLayTimeBerth_HoseOff,
					sfb.LayTimeCompleteLoad_HoseOff,
					sfb.AverageLayTimeCompleteLoad_HoseOff,
					sfb.LayTimeCompleteDischarge_HoseOff,
					sfb.AverageLayTimeCompleteDischarge_HoseOff,
					sfb.LaytimeCommenceLoad_CompleteLoad,
					sfb.AverageLaytimeCommenceLoad_CompleteLoad,
					sfb.LaytimeCommenceDischarge_CompleteDischarge,
					sfb.AverageLaytimeCommenceDischarge_CompleteDischarge,
					sfb.LayTimePumpingTime,
					sfb.AverageLayTimePumpingTime,
					sfb.LayTimePumpingRate,
					sfb.AverageLayTimePumpingRate,
					sfb.ParcelQuantity,
					sfb.LaytimeActual,
					sfb.AverageLaytimeActual,
					sfb.LaytimeAllowed,
					sfb.AverageLaytimeAllowed,
					sfb.PumpTime,
					sfb.AveragePumpTime,
					sfb.WithinLaycanOriginal,
					sfb.LaycanOverUnderOriginal,
					sfb.WithinLaycanFinal,
					sfb.LaycanOverUnderFinal,
					sfb.VoyageDuration,
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