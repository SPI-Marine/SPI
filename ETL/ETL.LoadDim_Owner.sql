/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the LoadDim_Owner stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Owner;
go

create procedure ETL.LoadDim_Owner
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Owner', 'U') is not null
		truncate table Staging.Dim_Owner;

	begin try
		insert
				Staging.Dim_Owner with (tablock)
		select
				ownerparent.QBRecId,
				ownerparent.OwnerParentName,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				OwnerParents ownerparent with (nolock)
					left join	(
									select
											@ExistingRecord RecordStatus,
											OwnerAlternateKey
										from
											Warehouse.Dim_Owner with (tablock)
								) rs
						on ownerparent.QBRecId = rs.OwnerAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging Owner records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Owner with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													OwnerName
												);
		
		update
				Staging.Dim_Owner with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Owner wp with (nolock)
			where
				wp.OwnerAlternateKey = Staging.Dim_Owner.OwnerAlternateKey
				and wp.Type1HashValue <> Staging.Dim_Owner.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Owners into Warehouse table
	begin try
		insert
				Warehouse.Dim_Owner with (tablock)
			select
					wo.OwnerAlternateKey,
					wo.OwnerName,
					wo.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Owner wo with (nolock)
				where
					wo.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Owner with (tablock)
			set
				OwnerName = so.OwnerName,
				Type1HashValue = so.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Owner so with (nolock)
			where
				so.RecordStatus & @ExistingRecord = @ExistingRecord
				and so.OwnerAlternateKey = Warehouse.Dim_Owner.OwnerAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Owner with (tablock)
			where
				not exists	(
								select
										1
									from
										OwnerParents op with (nolock)
									where
										op.QBRecId = OwnerAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Owner where OwnerKey < 0)
		begin
			delete
					Warehouse.Dim_Owner with (tablock)
				where
					OwnerKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Owner on;
			insert
					Warehouse.Dim_Owner with (tablock)	(
															OwnerKey,
															OwnerAlternateKey,
															OwnerName,
															Type1HashValue,
															RowCreatedDate,
															RowUpdatedDate,
															IsCurrentRow
														)

				values	(
							-1,				-- OwnerKey
							-1,				-- OwnerAlternateKey
							'Unknown',		-- OwnerName
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Owner off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end