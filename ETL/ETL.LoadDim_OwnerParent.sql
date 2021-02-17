set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_OwnerParent;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/15/2020
Description:	Creates the LoadDim_OwnerParent stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadDim_OwnerParent
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_OwnerParent', 'U') is not null
		truncate table Staging.Dim_OwnerParent;

	begin try
		insert
				Staging.Dim_OwnerParent with (tablock)
		select
				ownerparent.QBRecId,
				'op_' + convert(varchar(50), ownerparent.QBRecId) OwnerParentRlsKey,
				ownerparent.OwnerParentName,
				ownerparent.Notes,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				OwnerParents ownerparent with (nolock)
					left join	(
									select
											@ExistingRecord RecordStatus,
											OwnerParentAlternateKey
										from
											Warehouse.Dim_OwnerParent with (nolock)
								) rs
						on ownerparent.QBRecId = rs.OwnerParentAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging OwnerParent records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_OwnerParent with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																OwnerParentRlsKey,
																OwnerParentName,
																Notes
															)
												);
		
		update
				Staging.Dim_OwnerParent with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_OwnerParent wp with (nolock)
			where
				wp.OwnerParentAlternateKey = Staging.Dim_OwnerParent.OwnerParentAlternateKey
				and wp.Type1HashValue <> Staging.Dim_OwnerParent.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Owners into Warehouse table
	begin try
		insert
				Warehouse.Dim_OwnerParent with (tablock)
			select
					wo.OwnerParentAlternateKey,
					wo.OwnerParentRlsKey,
					wo.OwnerParentName,
					wo.Notes,
					wo.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_OwnerParent wo with (nolock)
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
				Warehouse.Dim_OwnerParent with (tablock)
			set
				OwnerParentRlsKey = so.OwnerParentRlsKey,
				OwnerParentName = so.OwnerParentName,
				Notes = so.Notes,
				Type1HashValue = so.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_OwnerParent so with (nolock)
			where
				so.RecordStatus & @ExistingRecord = @ExistingRecord
				and so.OwnerParentAlternateKey = Warehouse.Dim_OwnerParent.OwnerParentAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_OwnerParent with (tablock)
			where
				not exists	(
								select
										1
									from
										OwnerParents op with (nolock)
									where
										op.QBRecId = OwnerParentAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_OwnerParent where OwnerParentKey < 0)
		begin
			delete
					Warehouse.Dim_OwnerParent with (tablock)
				where
					OwnerParentKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_OwnerParent on;
			insert
					Warehouse.Dim_OwnerParent with (tablock)	(
																	OwnerParentKey,
																	OwnerParentAlternateKey,
																	OwnerParentRlsKey,
																	OwnerParentName,
																	Notes,
																	Type1HashValue,
																	RowCreatedDate,
																	RowUpdatedDate,
																	IsCurrentRow
																)

				values	(
							-1,				-- OwnerParentKey
							-1,				-- OwnerParentAlternateKey
							'Unknown',		-- OwnerParentRlsKey
							'Unknown',		-- OwnerParentName
							'Unknown',		-- Notes
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_OwnerParent off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end