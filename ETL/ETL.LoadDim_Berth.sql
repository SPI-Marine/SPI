/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/28/2018
Description:	Creates the LoadDim_Berth stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Berth;
go

create procedure ETL.LoadDim_Berth
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Berth', 'U') is not null
		truncate table Staging.Dim_Berth;

	begin try
		insert
				Staging.Dim_Berth
		select
			distinct
				berth.QBRecId,
				berth.BerthName,
				berth.DraftRestriction,
				berth.LOARestriction,
				berth.ProductRestriction,
				berth.ExNames,
				berth.UniqueId,
				berth.UpriverPorts,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				Berths berth
					left join	(
									select
											@ExistingRecord RecordStatus,
											BerthAlternateKey
										from
											Warehouse.Dim_Berth
								) rs
						on rs.BerthAlternateKey = berth.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging Berth records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Berth
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																BerthName,
																DraftRestriction,
																LOARestriction,
																ProductRestriction,
																ExNames,
																UniqueId,
																UpriverPorts
															)
												);
		
		update
				Staging.Dim_Berth
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Berth wb
			where
				wb.BerthAlternateKey = Staging.Dim_Berth.BerthAlternateKey
				and wb.Type1HashValue <> Staging.Dim_Berth.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new berths into Warehouse table
	begin try
		insert
				Warehouse.Dim_Berth
			select
					berth.BerthAlternateKey,
					berth.BerthName,
					berth.DraftRestriction,
					berth.LOARestriction,
					berth.ProductRestriction,
					berth.ExNames,
					berth.UniqueId,
					berth.UpRiverPorts,
					berth.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Berth berth
				where
					berth.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Berth
			set
				BerthName = berth.BerthName,
				DraftRestriction = berth.DraftRestriction,
				LOARestriction = berth.LOARestriction,
				ProductRestriction = berth.ProductRestriction,
				ExNames = berth.ExNames,
				UniqueId = berth.UniqueId,
				UpRiverPorts = berth.UpRiverPorts,
				Type1HashValue = berth.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Berth berth
			where
				berth.RecordStatus & @ExistingRecord = @ExistingRecord
				and berth.BerthAlternateKey = Warehouse.Dim_Berth.BerthAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if not exists (select 1 from Warehouse.Dim_Berth where BerthKey = -1)
		begin
			set identity_insert Warehouse.Dim_Berth on;
			insert
					Warehouse.Dim_Berth	(
														BerthKey,
														BerthAlternateKey,
														BerthName,
														DraftRestriction,
														LOARestriction,
														ProductRestriction,
														ExNames,
														UniqueId,
														UpRiverPorts,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,				-- BerthKey
							0,				-- BerthAlternateKey
							'Unknown',		-- BerthName
							0.0,			-- DraftRestriction
							0.0,			-- LOARestriction
							'Unknown',		-- ProductRestriction
							'Unknown',		-- ExNames
							'Unknown',		-- UniqueId
							'Unknown',		-- UpRiverPorts
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Berth off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end