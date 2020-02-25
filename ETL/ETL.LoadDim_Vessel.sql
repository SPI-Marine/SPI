/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the LoadDim_Vessel stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/20/2020	Added TankCoating ETL Logic
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Vessel;
go

create procedure ETL.LoadDim_Vessel
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Vessel', 'U') is not null
		truncate table Staging.Dim_Vessel;

	begin try
		insert
				Staging.Dim_Vessel
		select
			distinct
				vessel.QBRecId,
				vessel.VesselName,
				try_convert(decimal(18, 2), vessel.Draft),
				vessel.YearBuilt,
				vessel.Coils,
				vessel.DeadWeight,
				vessel.Beam,
				vessel.LOA,
				vessel.Yard,
				vessel.IceEntry,
				vessel.RegisteredOwner,
				vessel.CleanDirty,
				vessel.TcCandidate,
				vessel.IMOType,
				vessel.Tanks,
				vessel.Pumps,
				vessel.Segs,
				vessel.CBM,
				vessel.Hull,
				vessel.ExName,
				vessel.Comments,
				vessel.Trade,
				vessel.CountryOfBuild,
				vessel.Flag,
				vessel.DataSource,
				vessel.CommercialOwnerOperator,
				vessel.ArchivedVsl,
				vessel.ReasonForArchive,
				vessel.IGSType,
				vessel.[Status],
				vessel.IMOCertificateRemoved,
				vessel.KTRNumber,
				vessel.STSTCBM,
				vessel.MarineLineCBM,
				vessel.InterlineCBM,
				vessel.EpoxyCBM,
				vessel.ZincCBM,
				vessel.IMO1CBM,
				vessel.IMO2CBM,
				vessel.IMO3CBM,
				vessel.YdNo,
				vessel.NBContractDate,
				vessel.RetiredDate,
				vessel.KTRChangeDate,
				vessel.DeliveryDate,
				vessel.VesselType,
				tc.[Type]							TankCoating,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				Vessels vessel with (nolock)
					left join TankCoatings tc
						on tc.QBRecId = vessel.RelatedCoatingId
					left join	(
									select
											@ExistingRecord RecordStatus,
											VesselAlternateKey
										from
											Warehouse.Dim_Vessel with (nolock)
								) rs
						on rs.VesselAlternateKey = vessel.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging Vessel records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Vessel with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																VesselName,
																Draft,
																YearBuilt,
																Coils,
																DeadWeight,
																Beam,
																LOA,
																Yard,
																IceEntry,
																RegisteredOwner,
																CleanDirty,
																TcCandidate,
																IMOType,
																Tanks,
																Pumps,
																Segs,
																CBM,
																Hull,
																ExName,
																Comments,
																Trade,
																CountryOfBuild,
																Flag,
																DataSource,
																CommercialOwnerOperator,
																ArchivedVsl,
																ReasonForArchive,
																IGSType,
																[Status],
																IMOCertificateRemoved,
																KTRNumber,
																STSTCBM,
																MarineLineCBM,
																InterlineCBM,
																EpoxyCBM,
																ZincCBM,
																IMO1CBM,
																IMO2CBM,
																IMO3CBM,
																YdNo,
																NBContractDate,
																RetiredDate,
																KTRChangeDate,
																DeliveryDate,
																VesselType,
																TankCoating
															)
												);
		
		update
				Staging.Dim_Vessel with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Vessel wv with (nolock)
			where
				wv.VesselAlternateKey = Staging.Dim_Vessel.VesselAlternateKey
				and wv.Type1HashValue <> Staging.Dim_Vessel.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new vessels into Warehouse table
	begin try
		insert
				Warehouse.Dim_Vessel with (tablock)
			select
					vessel.VesselAlternateKey,
					vessel.VesselName,
					vessel.Draft,
					vessel.YearBuilt,
					vessel.Coils,
					vessel.DeadWeight,
					vessel.Beam,
					vessel.LOA,
					vessel.Yard,
					vessel.IceEntry,
					vessel.RegisteredOwner,
					vessel.CleanDirty,
					vessel.TcCandidate,
					vessel.IMOType,
					vessel.Tanks,
					vessel.Pumps,
					vessel.Segs,
					vessel.CBM,
					vessel.Hull,
					vessel.ExName,
					vessel.Comments,
					vessel.Trade,
					vessel.CountryOfBuild,
					vessel.Flag,
					vessel.DataSource,
					vessel.CommercialOwnerOperator,
					vessel.ArchivedVsl,
					vessel.ReasonForArchive,
					vessel.IGSType,
					vessel.[Status],
					vessel.IMOCertificateRemoved,
					vessel.KTRNumber,
					vessel.STSTCBM,
					vessel.MarineLineCBM,
					vessel.InterlineCBM,
					vessel.EpoxyCBM,
					vessel.ZincCBM,
					vessel.IMO1CBM,
					vessel.IMO2CBM,
					vessel.IMO3CBM,
					vessel.YdNo,
					vessel.NBContractDate,
					vessel.RetiredDate,
					vessel.KTRChangeDate,
					vessel.DeliveryDate,
					vessel.VesselType,
					Vessel.TankCoating,
					vessel.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Vessel vessel with (nolock)
				where
					vessel.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Vessel with (tablock)
			set
				VesselName = vessel.VesselName,
				Draft = vessel.Draft,
				YearBuilt = vessel.YearBuilt,
				Coils = vessel.Coils,
				DeadWeight = vessel.DeadWeight,
				Beam = vessel.Beam,
				LOA = vessel.LOA,
				Yard = vessel.Yard,
				IceEntry = vessel.IceEntry,
				RegisteredOwner = vessel.RegisteredOwner,
				CleanDirty = vessel.CleanDirty,
				TcCandidate = vessel.TcCandidate,
				IMOType = vessel.IMOType,
				Tanks = vessel.Tanks,
				Pumps = vessel.Pumps,
				Segs = vessel.Segs,
				CBM = vessel.CBM,
				Hull = vessel.Hull,
				ExName = vessel.ExName,
				Comments = vessel.Comments,
				Trade = vessel.Trade,
				CountryOfBuild = vessel.CountryOfBuild,
				Flag = vessel.Flag,
				DataSource = vessel.DataSource,
				CommercialOwnerOperator = vessel.CommercialOwnerOperator,
				ArchivedVsl = vessel.ArchivedVsl,
				ReasonForArchive = vessel.ReasonForArchive,
				IGSType = vessel.IGSType,
				[Status] = vessel.[Status],
				IMOCertificateRemoved = vessel.IMOCertificateRemoved,
				KTRNumber = vessel.KTRNumber,
				STSTCBM = vessel.STSTCBM,
				MarineLineCBM = vessel.MarineLineCBM,
				InterlineCBM = vessel.InterlineCBM,
				EpoxyCBM = vessel.EpoxyCBM,
				ZincCBM = vessel.ZincCBM,
				IMO1CBM = vessel.IMO1CBM,
				IMO2CBM = vessel.IMO2CBM,
				IMO3CBM = vessel.IMO3CBM,
				YdNo = vessel.YdNo,
				NBContractDate = vessel.NBContractDate,
				RetiredDate = vessel.RetiredDate,
				KTRChangeDate = vessel.KTRChangeDate,
				DeliveryDate = vessel.DeliveryDate,
				VesselType = vessel.VesselType,
				TankCoating = vessel.TankCoating,
				Type1HashValue = vessel.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Vessel vessel with (nolock)
			where
				vessel.RecordStatus & @ExistingRecord = @ExistingRecord
				and vessel.VesselAlternateKey = Warehouse.Dim_Vessel.VesselAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Vessel with (tablock)
			where
				not exists	(
								select
										1
									from
										Vessels v with (nolock)
									where
										v.QBRecId = VesselAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Vessel where VesselKey = -1)
		begin
			delete
					Warehouse.Dim_Vessel with (tablock)
				where
					VesselKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Vessel on;
			insert
					Warehouse.Dim_Vessel with (Tablock)	(
															VesselKey,
															VesselAlternateKey,
															VesselName,
															Draft,
															YearBuilt,
															Coils,
															DeadWeight,
															Beam,
															LOA,
															Yard,
															IceEntry,
															RegisteredOwner,
															CleanDirty,
															TcCandidate,
															IMOType,
															Tanks,
															Pumps,
															Segs,
															CBM,
															Hull,
															ExName,
															Comments,
															Trade,
															CountryOfBuild,
															Flag,
															DataSource,
															CommercialOwnerOperator,
															ArchivedVsl,
															ReasonForArchive,
															IGSType,
															[Status],
															IMOCertificateRemoved,
															KTRNumber,
															STSTCBM,
															MarineLineCBM,
															InterlineCBM,
															EpoxyCBM,
															ZincCBM,
															IMO1CBM,
															IMO2CBM,
															IMO3CBM,
															YdNo,
															NBContractDate,
															RetiredDate,
															KTRChangeDate,
															DeliveryDate,
															VesselType,
															TankCoating,
															Type1HashValue,
															RowCreatedDate,
															RowUpdatedDate,
															IsCurrentRow
														)

				values	(
							-1,				-- VesselKey
							0,				-- VesselAlternateKey
							'Unknown',		-- VesselName
							0.0,			-- Draft
							0,				-- YearBuilt
							'Unknown',		-- Coils
							0.0,			-- DeadWeight
							0.0,			-- Beam
							0.0,			-- LOA
							'Unknown',		-- Yard
							'Unknown',		-- IceEntry
							'Unknown',		-- RegisteredOwner
							'Unknown',		-- CleanDirty
							0,				-- TcCandidate
							'Unknown',		-- IMOType
							0.0,			-- Tanks
							0.0,			-- Pumps
							0.0,			-- Segs
							0.0,			-- CBM
							'Unknown',		-- Hull
							'Unknown',		-- ExName
							'Unknown',		-- Comments
							'Unknown',		-- Trade
							'Unknown',		-- CountryOfBuild
							'Unknown',		-- Flag
							'Unknown',		-- DataSource
							'Unknown',		-- CommercialOwnerOperator
							0,				-- ArchivedVsl
							'Unknown',		-- ReasonForArchive
							'Unknown',		-- IGSType
							'Unknown',		-- [Status]
							'Unknown',		-- IMOCertificateRemoved
							'Unknown',		-- KTRNumber
							0.0,			-- STSTCBM
							0.0,			-- MarineLineCBM
							0.0,			-- InterlineCBM
							0.0,			-- EpoxyCBM
							0.0,			-- ZincCBM
							0.0,			-- IMO1CBM
							0.0,			-- IMO2CBM
							0.0,			-- IMO3CBM
							'Unknown',		-- YdNo
							'12/30/1899',	-- NBContractDate
							'12/30/1899',	-- RetiredDate
							'12/30/1899',	-- KTRChangeDate
							'12/30/1899',	-- DeliveryDate
							'Unknown',		-- VesselType
							'Unknown',		-- TankCoating
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Vessel off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end