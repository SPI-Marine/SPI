set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_TradeLane;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/15/2021
Description:	Creates the LoadDim_TradeLane stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadDim_TradeLane
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_TradeLane', 'U') is not null
		truncate table Staging.Dim_TradeLane;

	begin try
		insert
				Staging.Dim_TradeLane with (tablock)
		select
				tl.RecordID								TradeLaneAlternateKey,
				isnull(coa.COAKey, -1)					COAKey,
				tl.TradelaneNumLiftingsMin_Entry		TradelaneNumLiftingsMinEntry,
				tl.TradelaneNumLiftingsMax_Entry		TradelaneNumLiftingsMaxEntry,
				tl.TradeLaneLiftingQtyMin_Entry			TradeLaneLiftingQtyMinEntry,
				tl.TradeLaneLiftingQtyMax_Entry			TradeLaneLiftingQtyMaxEntry,
				tl.LoadOption,
				tl.FreightDetails,
				tl.LiftingRequirementOptions,
				tl.TradeLaneTitle,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord)		RecordStatus
			from
				TradeLane tl (nolock)
					left join Warehouse.Dim_COA coa (nolock)
						on coa.COAAlternateKey = tl.RelatedSPICOAID
					left join	(
									select
											@ExistingRecord RecordStatus,
											TradeLaneAlternateKey
										from
											Warehouse.Dim_TradeLane with (tablock)
								) rs
						on tl.RecordID = rs.TradeLaneAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging Trade Lane records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_TradeLane with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																convert(varchar(50), COAKey),
																convert(varchar(50), TradelaneNumLiftingsMinEntry),
																convert(varchar(50), TradelaneNumLiftingsMaxEntry),
																convert(varchar(50), TradeLaneLiftingQtyMinEntry),
																convert(varchar(50), TradeLaneLiftingQtyMaxEntry),
																LoadOption,
																FreightDetails,
																LiftingRequirementOptions,
																TradeLaneTitle
															)
												);
		
		update
				Staging.Dim_TradeLane with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_TradeLane wtl with (nolock)
			where
				wtl.TradeLaneAlternateKey = Staging.Dim_TradeLane.TradeLaneAlternateKey
				and wtl.Type1HashValue <> Staging.Dim_TradeLane.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new COAs into Warehouse table
	begin try
		insert
				Warehouse.Dim_TradeLane with (tablock)
			select
					tl.TradeLaneAlternateKey,
					tl.COAKey,
					tl.TradelaneNumLiftingsMinEntry,
					tl.TradelaneNumLiftingsMaxEntry,
					tl.TradeLaneLiftingQtyMinEntry,
					tl.TradeLaneLiftingQtyMaxEntry,
					tl.LoadOption,
					tl.FreightDetails,
					tl.LiftingRequirementOptions,
					tl.TradeLaneTitle,
					tl.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_TradeLane tl with (nolock)
				where
					tl.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_TradeLane with (tablock)
			set
				COAKey = tl.COAKey,
				TradelaneNumLiftingsMinEntry = tl.TradelaneNumLiftingsMinEntry,
				TradelaneNumLiftingsMaxEntry = tl.TradelaneNumLiftingsMaxEntry,
				TradeLaneLiftingQtyMinEntry = tl.TradeLaneLiftingQtyMinEntry,
				TradeLaneLiftingQtyMaxEntry = tl.TradeLaneLiftingQtyMaxEntry,
				LoadOption = tl.LoadOption,
				FreightDetails = tl.FreightDetails,
				LiftingRequirementOptions = tl.LiftingRequirementOptions,
				TradeLaneTitle = tl.TradeLaneTitle,
				Type1HashValue = tl.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_TradeLane tl with (nolock)
			where
				tl.RecordStatus & @ExistingRecord = @ExistingRecord
				and tl.TradeLaneAlternateKey = Warehouse.Dim_TradeLane.TradeLaneAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_TradeLane with (tablock)
			where
				not exists	(
								select
										1
									from
										TradeLane tl with (nolock)
									where
										tl.RecordID = TradeLaneAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_TradeLane where TradeLaneKey < 0)
		begin
			delete
					Warehouse.Dim_TradeLane with (tablock)
				where
					TradeLaneKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_TradeLane on;
			insert
					Warehouse.Dim_TradeLane with (tablock)	(
																TradeLaneKey,
																TradeLaneAlternateKey,
																COAKey,
																TradelaneNumLiftingsMinEntry,
																TradelaneNumLiftingsMaxEntry,
																TradeLaneLiftingQtyMinEntry,
																TradeLaneLiftingQtyMaxEntry,
																LoadOption,
																FreightDetails,
																LiftingRequirementOptions,
																TradeLaneTitle,
																Type1HashValue,
																RowCreatedDate,
																RowUpdatedDate,
																IsCurrentRow
															)

				values	(
							-1,				-- TradeLaneKey
							0,				-- TradeLaneAlternateKey
							0,				-- COAKey
							0,				-- TradelaneNumLiftingsMinEntry
							0,				-- TradelaneNumLiftingsMaxEntry
							0,				-- TradeLaneLiftingQtyMinEntry
							0,				-- TradeLaneLiftingQtyMaxEntry
							'Unknown',		-- LoadOption
							'Unknown',		-- FreightDetails
							'Unknown',		-- LiftingRequirementOptions
							'Unknown',		-- TradeLaneTitle
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_TradeLane off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end