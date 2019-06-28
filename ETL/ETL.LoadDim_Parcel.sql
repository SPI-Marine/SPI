set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Parcel;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/21/2019
Description:	Creates the LoadDim_Parcel stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	05/20/2019	Remove deleted records from Warehouse
Brian Boswick	06/04/2019	Added DeadfreightQty
==========================================================================================================	
*/

create procedure ETL.LoadDim_Parcel
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Parcel', 'U') is not null
		truncate table Staging.Dim_Parcel;

	begin try
		insert
				Staging.Dim_Parcel with (tablock)
		select
			distinct
				parcel.QBRecId								ParcelAlternateKey,
				parcel.BillLadingDate						BillLadingDate,
				parcel.ParcelFrtRate						ParcelFrtRate,
				parcel.OutTurnQty							OutTurnQty,
				parcel.ShipLoadedQty						ShipLoadedQty,
				parcel.ShipDischargeQty						ShipDischargeQty,
				parcel.NominatedQty							NominatedQty,
				parcel.BLQty								BLQty,
				parcel.Comments								Comments,
				parcel.Unit									Unit,
				parcel.DemurrageAgreedAmount_QBC			AgreedDemurrage,
				parcel.DemurrageClaimAmount_QBC				ClaimDemurrage,
				parcel.DemurrageVaultEstimateAmount_QBC		VaultDemurrage,
				parcel.[DemurrageAgreedPro-ration_QBC]		IsAgreedProRated,
				null										ParcelNumber,
				parcel.DeadfreightQty						DeadfreightQty,
				0 											Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord)			RecordStatus
			from
				Parcels parcel
					left join	(
									select
											@ExistingRecord RecordStatus,
											ParcelAlternateKey
										from
											Warehouse.Dim_Parcel
								) rs
						on rs.ParcelAlternateKey = parcel.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging Parcel records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Update ParcelNumber
	begin try
		update
				Staging.Dim_Parcel with (tablock)
			set
				ParcelNumber = parcelnumber.ParcelNumber
			from
				Parcels parcel
					left join	(
									select
											p.QbRecId					ParcelAltKey,
											p.RelatedSpiFixtureId,
											row_number() over	(
																	partition by p.RelatedSpiFixtureId
																	order by p.QbRecId
																)		ParcelNumber
										from
											Parcels p
										where
											p.RelatedSpiFixtureId is not null
								) parcelnumber
						on parcelnumber.ParcelAltKey = parcel.QbRecId
			where
				parcel.QbRecId = Staging.Dim_Parcel.ParcelAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Parcel with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																BillLadingDate,
																ParcelFrtRate,
																OutTurnQty,
																ShipLoadedQty,
																ShipDischargeQty,
																NominatedQty,
																BLQty,
																Comments,
																Unit,
																AgreedDemurrage,
																ClaimDemurrage,
																VaultDemurrage,
																IsAgreedProRated,
																ParcelNumber,
																DeadfreightQty
															)
												);
		
		update
				Staging.Dim_Parcel with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Parcel wb
			where
				wb.ParcelAlternateKey = Staging.Dim_Parcel.ParcelAlternateKey
				and wb.Type1HashValue <> Staging.Dim_Parcel.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new berths into Warehouse table
	begin try
		insert
				Warehouse.Dim_Parcel with (tablock)
			select
					parcel.ParcelAlternateKey,
					parcel.BillLadingDate,
					parcel.ParcelFrtRate,
					parcel.OutTurnQty,
					parcel.ShipLoadedQty,
					parcel.ShipDischargeQty,
					parcel.NominatedQty,
					parcel.BLQty,
					parcel.Comments,
					parcel.Unit,
					parcel.AgreedDemurrage,
					parcel.ClaimDemurrage,
					parcel.VaultDemurrage,
					parcel.IsAgreedProRated,
					parcel.ParcelNumber,
					parcel.DeadfreightQty,
					parcel.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Parcel parcel
				where
					parcel.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Parcel
			set
				BillLadingDate = parcel.BillLadingDate,
				ParcelFrtRate = parcel.ParcelFrtRate,
				OutTurnQty = parcel.OutTurnQty,
				ShipLoadedQty = parcel.ShipLoadedQty,
				ShipDischargeQty = parcel.ShipDischargeQty,
				NominatedQty = parcel.NominatedQty,
				BLQty = parcel.BLQty,
				Comments = parcel.Comments,
				Unit = parcel.Unit,
				AgreedDemurrage = parcel.AgreedDemurrage,
				ClaimDemurrage = parcel.ClaimDemurrage,
				VaultDemurrage = parcel.VaultDemurrage,
				IsAgreedProRated = parcel.IsAgreedProRated,
				ParcelNumber = parcel.ParcelNumber,
				DeadfreightQty = parcel.DeadfreightQty,
				Type1HashValue = parcel.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Parcel parcel
			where
				parcel.RecordStatus & @ExistingRecord = @ExistingRecord
				and parcel.ParcelAlternateKey = Warehouse.Dim_Parcel.ParcelAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	--begin try
	--	delete
	--			Warehouse.Dim_Parcel
	--		where
	--			not exists	(
	--							select
	--									1
	--								from
	--									Parcels p
	--								where
	--									p.QBRecId = ParcelAlternateKey
	--						);
	--end try
	--begin catch
	--	select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
	--	throw 51000, @ErrorMsg, 1;
	--end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Parcel where ParcelKey in (-1, -2))
		begin
			delete
					Warehouse.Dim_Parcel
				where
					ParcelKey in (-1, -2);
		end
		else
		begin
			set identity_insert Warehouse.Dim_Parcel on;
			insert
					Warehouse.Dim_Parcel	(
														ParcelKey,
														ParcelAlternateKey,
														BillLadingDate,
														ParcelFrtRate,
														OutTurnQty,
														ShipLoadedQty,
														ShipDischargeQty,
														NominatedQty,
														BLQty,
														Comments,
														Unit,
														AgreedDemurrage,
														ClaimDemurrage,
														VaultDemurrage,
														IsAgreedProRated,
														ParcelNumber,
														DeadfreightQty,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,							-- ParcelKey
							0,							-- ParcelAlternateKey
							'12/30/1899',				-- BillLadingDate
							0.0,						-- ParcelFrtRate
							0.0,						-- OutTurnQty
							0.0,						-- ShipLoadedQty
							0.0,						-- ShipDischargeQty
							0.0,						-- NominatedQty
							0.0,						-- BLQty
							'Unknown',					-- Comments
							'Unknown',					-- Unit
							0.0,						-- AgreedDemurrage
							0.0,						-- ClaimDemurrage
							0.0,						-- VaultDemurrage
							'Unknown',					-- IsAgreedProRated
							0,							-- ParcelNumber
							0,							-- DeadfreightQty
							0,							-- Type1HashValue
							getdate(),					-- RowCreatedDate
							getdate(),					-- RowUpdatedDate
							'Y'							-- IsCurrentRow
						),
						(
							-2,							-- ParcelKey
							0,							-- ParcelAlternateKey
							'12/30/1899',				-- BillLadingDate
							0.0,						-- ParcelFrtRate
							0.0,						-- OutTurnQty
							0.0,						-- ShipLoadedQty
							0.0,						-- ShipDischargeQty
							0.0,						-- NominatedQty
							0.0,						-- BLQty
							'No Parcel Associated',		-- Comments
							'Unknown',					-- Unit
							0.0,						-- AgreedDemurrage
							0.0,						-- ClaimDemurrage
							0.0,						-- VaultDemurrage
							'Unknown',					-- IsAgreedProRated
							0,							-- ParcelNumber
							0,							-- DeadfreightQty
							0,							-- Type1HashValue
							getdate(),					-- RowCreatedDate
							getdate(),					-- RowUpdatedDate
							'Y'							-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Parcel off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end