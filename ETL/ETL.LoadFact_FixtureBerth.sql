/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/06/2019
Description:	Creates the LoadFact_FixtureBerth stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_FixtureBerth;
go

create procedure ETL.LoadFact_FixtureBerth
as
begin
	set nocount on;

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
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
					Parcels p
						join ParcelPorts loadport
							on p.RelatedLoadPortID = loadport.QBRecId
						join ParcelBerths loadberth
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
						join ParcelPorts dischport
							on p.RelatedDischPortId = dischport.QBRecId
						join ParcelBerths dischberth
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
												LoadDischarge,
												ParcelQuantity,
												LaytimeAllowed,
												RecordStatus
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
					ufb.LoadDischarge							LoadDischarge,
					ufb.ParcelQuantity							ParcelQuantity,
					ufb.LaytimeAllowed							LaytimeAllowed,
					isnull(rs.RecordStatus, @NewRecord)			RecordStatus
				from
					AggregatedParcelQuantity ufb
						left join Warehouse.Dim_PostFixture wpostfixture
							on wpostfixture.PostFixtureAlternateKey = ufb.PostFixtureAlternateKey
						left join PostFixtures epostfixture
							on epostfixture.QBRecId = wpostfixture.PostFixtureAlternateKey
						left join Warehouse.Dim_Vessel vessel
							on vessel.VesselAlternateKey = epostfixture.RelatedVessel
						left join Warehouse.Dim_PortBerth portberth
							on portberth.PortAlternateKey = ufb.PortAlternateKey
								and portberth.BerthAlternateKey = ufb.BerthAlternateKey
						left join	(
										select
												@ExistingRecord RecordStatus,
												PostFixtureAlternateKey,
												PortAlternateKey,
												BerthAlternateKey,
												LoadDischargeAlternateKey,
												ParcelBerthAlternateKey
											from
												Warehouse.Fact_FixtureBerth
									) rs
							on rs.PostFixtureAlternateKey = ufb.PostFixtureAlternateKey
								and rs.PortAlternateKey = ufb.PortAlternateKey
								and rs.BerthAlternateKey = ufb.BerthAlternateKey
								and rs.LoadDischargeAlternateKey = ufb.LoadDischargeAlternateKey
								and rs.ParcelBerthAlternateKey = ufb.ParcelBerthAlternateKey;
	end try
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
							SOFEvents e
								join ParcelBerths pb 
									on e.RelatedParcelBerthId = pb.QBRecId
						group by
							pb.RelatedSpiFixtureId
				) fe
					join Warehouse.Dim_Calendar wc
						on wc.FullDate = fe.FirstEventDate
			where
				fe.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating FirstEventDateKey - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ProductKey for the Post Fixture
	begin try
		with TopProductTypeQuantities	(
											PostFixtureAlternateKey,
											MaxQuantity,
											ProductType
										)
		as
		(
			select
					p.RelatedSpiFixtureId	PostFixtureAlternateKey,
					max(p.BLQty)			MaxQuantity,
					prodtype.TypeName		ProductType
				from
					Parcels p
						join ParcelProducts pp
							on p.RelatedParcelProductId = pp.QBRecId
						join Products prod
							on pp.RelatedProductId = prod.QBRecId
						join ProductType prodtype
							on prod.RelatedProductTypeId = prodtype.QBRecId
				where
					p.RelatedSpiFixtureId is not null
				group by
					p.RelatedSpiFixtureId,
					prodtype.TypeName
		)
				
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				ProductType = prodtype.ProductType
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
					Warehouse.Fact_SOFEvent wevent
						join Warehouse.Dim_PostFixture wpf
							on wpf.PostFixtureKey = wevent.PostFixtureKey
						join SOFEvents e
							on wevent.EventAlternateKey = e.QBRecId
				where
					wevent.PostFixtureKey > 0
					and isnull(wevent.Duration, 0) > 0
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

	-- Update WaitTimeNOR_Berth
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeNOR_Berth = fbed.WaitTimeNOR_Berth
			from
				(
					select
							sum(fbe.Duration)				WaitTimeNOR_Berth,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 219	-- NOR Tendered
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 228	-- Berth/Alongside
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
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
				WaitTimeBerth_HoseOn = fbed.WaitTimeBerth_HoseOn
			from
				(
					select
							sum(fbe.Duration)				WaitTimeBerth_HoseOn,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 228	-- Berth/Alongside
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 257	-- Hose Connected
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update WaitTimeBerth_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				WaitTimeBerth_HoseOff = fbed.WaitTimeBerth_HoseOff
			from
				(
					select
							sum(fbe.Duration)				WaitTimeBerth_HoseOff,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 228	-- Berth/Alongside
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 260	-- Hose Disconnected
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
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
				WaitTimeCompleteLoad_HoseOff = fbed.WaitTimeCompleteLoad_HoseOff
			from
				(
					select
							sum(fbe.Duration)				WaitTimeCompleteLoad_HoseOff,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 265	-- Completed Loading
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 260	-- Hose Disconnected
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
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
				WaitTimeCompleteDischarge_HoseOff = fbed.WaitTimeCompleteDischarge_HoseOff
			from
				(
					select
							sum(fbe.Duration)				WaitTimeCompleteDischarge_HoseOff,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 271	-- Complete Discharge
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 260	-- Hose Disconnected
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating WaitTimeCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeNOR_Berth
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LayTimeNOR_Berth = fbed.LayTimeNOR_Berth
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimeNOR_Berth,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 219	-- NOR Tendered
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
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
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LayTimeNOR_Berth - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeBerth_HoseOn
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LayTimeBerth_HoseOn = fbed.LayTimeBerth_HoseOn
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimeBerth_HoseOn,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 228	-- Berth/Alongside
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
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
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LayTimeBerth_HoseOn - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeBerth_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LayTimeBerth_HoseOff = fbed.LayTimeBerth_HoseOff
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimeBerth_HoseOff,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 228	-- Berth/Alongside
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
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
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LayTimeBerth_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeCompleteLoad_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LayTimeCompleteLoad_HoseOff = fbed.LayTimeCompleteLoad_HoseOff
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimeCompleteLoad_HoseOff,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 265	-- Completed Loading
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
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
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LayTimeCompleteLoad_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimeCompleteDischarge_HoseOff
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LayTimeCompleteDischarge_HoseOff = fbed.LayTimeCompleteDischarge_HoseOff
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimeCompleteDischarge_HoseOff,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.EventNum >=	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
														where
															en.EventTypeId = 271	-- Complete Discharge
															and en.PostFixtureAlternateKey = fbe.PostFixtureAlternateKey
															and en.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
												)
							
							and	fbe.EventNum <	(
													select
															min(EventNum)
														from
															Staging.Fact_FixtureBerthEvents en
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
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LayTimeCompleteDischarge_HoseOff - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LayTimePumpingTime
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LayTimePumpingTime = fbed.LayTimePumpingTime
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimePumpingTime,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.IsLayTime = 'Y'
							and fbe.IsPumpingTime = 'Y'
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
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
												then fbe.ParcelQuantity/fbed.LayTimePumpingTime
											else null
										end
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LayTimePumpingTime,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
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
				and fbed.ParcelBerthAlternateKey = fbe.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LayTimePumpingRate - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update LaytimeActual
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				LaytimeActual = fbed.LaytimeActual
			from
				(
					select
							sum(fbe.LayTimeUsedProrated)	LaytimeActual,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.IsLayTime = 'Y'
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating LaytimeActual - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update PumpTime
	begin try
		update
				Staging.Fact_FixtureBerth with (tablock)
			set
				PumpTime = fbed.PumpTime
			from
				(
					select
							sum(fbe.Duration)				PumpTime,
							fbe.PostFixtureAlternateKey		PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey		ParcelBerthAlternateKey
						from
							Staging.Fact_FixtureBerthEvents fbe
						where
							fbe.IsPumpingTime = 'Y'
						group by
							fbe.PostFixtureAlternateKey,
							fbe.ParcelBerthAlternateKey
				) fbed
			where
				fbed.PostFixtureAlternateKey = Staging.Fact_FixtureBerth.PostFixtureAlternateKey
				and fbed.ParcelBerthAlternateKey = Staging.Fact_FixtureBerth.ParcelBerthAlternateKey
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
					sfb.LoadDischarge,
					sfb.ProductType,
					sfb.ParcelQuantityTShirtSize,
					sfb.WaitTimeNOR_Berth,
					sfb.WaitTimeBerth_HoseOn,
					sfb.WaitTimeBerth_HoseOff,
					sfb.WaitTimeCompleteLoad_HoseOff,
					sfb.WaitTimeCompleteDischarge_HoseOff,
					sfb.LayTimeNOR_Berth,
					sfb.LayTimeBerth_HoseOn,
					sfb.LayTimeBerth_HoseOff,
					sfb.LayTimeCompleteLoad_HoseOff,
					sfb.LayTimeCompleteDischarge_HoseOff,
					sfb.LayTimePumpingTime,
					sfb.LayTimePumpingRate,
					sfb.ParcelQuantity,
					sfb.LaytimeActual,
					sfb.LaytimeAllowed,
					sfb.PumpTime,
					getdate() RowStartDate,
					getdate() RowUpdatedDate
				from
					Staging.Fact_FixtureBerth sfb
				where
					sfb.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end