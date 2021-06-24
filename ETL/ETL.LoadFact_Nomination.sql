set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_Nomination;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	06/23/2021
Description:	Creates the LoadFact_Nomination stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_Nomination
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_Nomination', 'U') is not null
		truncate table Staging.Fact_Nomination;

	begin try
		insert
				Staging.Fact_Nomination with (tablock)	(
															ParcelAlternateKey,
															NominationAlternateKey,
															PostFixtureKey,
															LoadPortKey,
															DischargePortKey,
															LoadBerthKey,
															DischargeBerthKey,
															LoadPortBerthKey,
															DischargePortBerthKey,
															ProductKey,
															BillLadingDateKey,
															DimParcelKey,
															VesselKey,
															COAKey,
															NominatedQty,
															BLQty,
															TentCargoNomOriginalQty,
															TentCargoNomDateOriginal,
															[Status],
															LoadPortAlternateKey,
															DischargePortAlternateKey,
															PostFixtureAlternateKey
														)
		select
				p.QbRecId										ParcelAlternateKey,
				nom.RecordID									NominationAlternateKey,
				isnull(wdpostfixture.PostFixtureKey, -1)		PostFixtureKey,
				isnull(wdloadport.PortKey, -1)					LoadPortKey,
				isnull(wddischport.PortKey, -1)					DischargePortKey,
				isnull(wdloadberth.BerthKey, -1)				LoadBerthKey,
				isnull(wddischberth.BerthKey, -1)				DischargeBerthKey,
				isnull(wdloadportberth.PortBerthKey, -1)		LoadPortBerthKey,
				isnull(wddischportberth.PortBerthKey, -1)		DischargePortBerthKey,
				isnull(wdproduct.ProductKey , -1)				ProductKey,
				isnull(bld.DateKey, -1)							BillLadingDateKey,
				wdparcel.ParcelKey								DimParcelKey,
				isnull(v.VesselKey, -1)							VesselKey,
				isnull(coa.COAKey, -1)							COAKey,
				p.NominatedQty,
				p.BLQty,
				case
					when isnull(nom.CargoNominationbyParcel, 'no') = 'yes'
						then p.TentativeCargoNomQty
					else nom.TentCargoNom_OriginalQty
				end												TentCargoNomOriginalQty,
				case
					when isnull(nom.CargoNominationbyParcel, 'no') = 'yesy'
						then p.TentativeCargoNomDateOriginal
					else nom.TentCargoNomDateOriginal
				end												TentCargoNomDateOriginal,
				p.[Status],
				wdloadport.PortAlternateKey						LoadPortAlternateKey,
				wddischport.PortAlternateKey					DischargePortAlternateKey,
				wdpostfixture.PostFixtureAlternateKey
			from
				Parcels p with (nolock)
					join Warehouse.Dim_Parcel wdparcel with (nolock)
						on wdparcel.ParcelAlternateKey = p.QbRecId
					join Nominations nom (nolock)
						on nom.RelatedSPIFixtureID = p.RelatedSpiFixtureId
					left join ParcelPorts loadparcelport with (nolock)
						on loadparcelport.QBRecId = p.RelatedLoadPortID
					left join Warehouse.Dim_Port wdloadport with (nolock)
						on wdloadport.PortAlternateKey = loadparcelport.RelatedPortId
					left join ParcelPorts dischparcelport with (nolock)
						on dischparcelport.QBRecId = p.RelatedDischPortId
					left join Warehouse.Dim_Port wddischport with (nolock)
						on wddischport.PortAlternateKey = dischparcelport.RelatedPortId
					left join ParcelBerths loadparcelberth with (nolock)
						on loadparcelberth.QBRecId = p.RelatedLoadBerth
					left join Warehouse.Dim_Berth wdloadberth with (nolock)
						on wdloadberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
					left join ParcelBerths dischparcelberth with (nolock)
						on dischparcelberth.QBRecId = p.RelatedDischBerth
					left join Warehouse.Dim_Berth wddischberth with (nolock)
						on wddischberth.BerthAlternateKey = dischparcelberth.RelatedBerthId
					left join Warehouse.Dim_PortBerth wdloadportberth with (nolock)
						on wdloadportberth.PortAlternateKey = loadparcelport.RelatedPortId
							and wdloadportberth.BerthAlternateKey = loadparcelberth.RelatedBerthId
					left join Warehouse.Dim_PortBerth wddischportberth with (nolock)
						on wddischportberth.PortAlternateKey = dischparcelport.RelatedPortId
							and wddischportberth.BerthAlternateKey = dischparcelberth.RelatedBerthId
					left join ParcelProducts parprod with (nolock)
						on parprod.QBRecId = p.RelatedParcelProductId
					left join Warehouse.Dim_Product wdproduct with (nolock)
						on wdproduct.ProductAlternateKey = parprod.RelatedProductId
					left join Warehouse.Dim_PostFixture wdpostfixture with (nolock)
						on p.RelatedSpiFixtureId = wdpostfixture.PostFixtureAlternateKey
					left join Warehouse.Dim_Calendar bld with (nolock)
						on bld.FullDate = convert(date, p.BillLadingDate)
					left join PostFixtures pf with (nolock)
						on p.RelatedSpiFixtureId = pf.QBRecId
					left join Warehouse.Dim_COA coa (nolock)
						on coa.COAAlternateKey = pf.RelatedSPICOAId
					left join Warehouse.Dim_Vessel v with (nolock)
						on v.VesselAlternateKey = pf.RelatedVessel;
	end try
	begin catch
		select @ErrorMsg = 'Staging Parcel records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Get NOR start dates for load/discharge ports
	begin try
		update
				Staging.Fact_Nomination with (tablock)
			set
				LoadNORStartDate = firstloadnorevent.FirstNOREventDate,
				LoadLastHoseOffDate = lastloadhoseoffevent.LastHoseOffEventDate,
				DischargeNORStartDate = firstdischargenorevent.FirstNOREventDate,
				DischargeLastHoseOffDate = lastdischargehoseoffevent.LastHoseOffEventDate
			from
				Staging.Fact_Nomination sfp
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
													pet.QBRecId = 219	-- NOR
													and pp.[Type] = 'Load'
												group by
													pf.QBRecId, pp.RelatedPortId
								) firstloadnorevent
						on firstloadnorevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and firstloadnorevent.RelatedPortID = sfp.LoadPortAlternateKey
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											max(e.StartDate)	LastHoseOffEventDate
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
													pet.QBRecId = 260	-- Hose Off Event
													and pp.[Type] = 'Load'
												group by
													pf.QBRecId, pp.RelatedPortId
								) lastloadhoseoffevent
						on lastloadhoseoffevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and lastloadhoseoffevent.RelatedPortID = sfp.LoadPortAlternateKey
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
													pet.QBRecId = 219	-- NOR
													and pp.[Type] = 'Discharge'
												group by
													pf.QBRecId, pp.RelatedPortId
								) firstdischargenorevent
						on firstdischargenorevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and firstdischargenorevent.RelatedPortID = sfp.DischargePortAlternateKey
					left join	(
									select
											pf.QBRecId			PostFixtureAlternateKey,
											pp.RelatedPortId	RelatedPortID,
											max(e.StartDate)	LastHoseOffEventDate
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
													pet.QBRecId = 260	-- Hose Off Event
													and pp.[Type] = 'Discharge'
												group by
													pf.QBRecId, pp.RelatedPortId
								) lastdischargehoseoffevent
						on lastdischargehoseoffevent.PostFixtureAlternateKey = sfp.PostFixtureAlternateKey
							and lastdischargehoseoffevent.RelatedPortID = sfp.DischargePortAlternateKey
	end try
	begin catch
		select @ErrorMsg = 'Updating NOR start dates for load/discharge ports - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_Nomination', 'U') is not null
		truncate table Warehouse.Fact_Nomination;

	-- Insert records into Warehouse table
	begin try
		insert
				Warehouse.Fact_Nomination with (tablock)	(
																ParcelAlternateKey,
																NominationAlternateKey,
																PostFixtureKey,
																LoadPortKey,
																DischargePortKey,
																LoadBerthKey,
																DischargeBerthKey,
																LoadPortBerthKey,
																DischargePortBerthKey,
																ProductKey,
																BillLadingDateKey,
																DimParcelKey,
																VesselKey,
																COAKey,
																NominatedQty,
																BLQty,
																TentCargoNomOriginalQty,
																LoadNORStartDate,
																LoadLastHoseOffDate,
																DischargeNORStartDate,
																DischargeLastHoseOffDate,
																TentCargoNomDateOriginal,
																[Status],
																RowCreatedDate
															)
			select
					sfp.ParcelAlternateKey,
					sfp.NominationAlternateKey,
					sfp.PostFixtureKey,
					sfp.LoadPortKey,
					sfp.DischargePortKey,
					sfp.LoadBerthKey,
					sfp.DischargeBerthKey,
					sfp.LoadPortBerthKey,
					sfp.DischargePortBerthKey,
					sfp.ProductKey,
					sfp.BillLadingDateKey,
					sfp.DimParcelKey,
					sfp.VesselKey,
					sfp.COAKey,
					sfp.NominatedQty,
					sfp.BLQty,
					sfp.TentCargoNomOriginalQty,
					sfp.LoadNORStartDate,
					sfp.LoadLastHoseOffDate,
					sfp.DischargeNORStartDate,
					sfp.DischargeLastHoseOffDate,
					sfp.TentCargoNomDateOriginal,
					sfp.[Status],
					getdate()
				from
					Staging.Fact_Nomination sfp with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end