set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_ChartererParent;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/15/2020
Description:	Creates the stored procedure LoadDim_ChartererParent
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadDim_ChartererParent
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_ChartererParent', 'U') is not null
		truncate table Staging.Dim_ChartererParent;

	begin try
		insert
				Staging.Dim_ChartererParent with (tablock)
		select
				charterer.QBRecId ChartererParentAlternateKey,
				'cp_' + convert(varchar(50), charterer.QBRecId) ChartererParentRlsKey,
				charterer.ChartererParentName,
				charterer.Notes,
				charterer.[Type],
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				ChartererParents charterer with (nolock)
					left join	(
									select
											@ExistingRecord RecordStatus,
											ChartererParentAlternateKey
										from
											Warehouse.Dim_ChartererParent with (nolock)
								) rs
						on rs.ChartererParentAlternateKey = charterer.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging ChartererParent records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_ChartererParent with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																ChartererParentRlsKey,
																ChartererParentName,
																Notes,
																[Type]
															)
												);
		
		update
				Staging.Dim_ChartererParent with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_ChartererParent wc with (nolock)
			where
				wc.ChartererParentAlternateKey = Staging.Dim_ChartererParent.ChartererParentAlternateKey
				and wc.Type1HashValue <> Staging.Dim_ChartererParent.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Charterers into Warehouse table
	begin try
		insert
				Warehouse.Dim_ChartererParent  with (tablock)
			select
					charterer.ChartererParentAlternateKey,
					charterer.ChartererParentRlsKey,
					charterer.ChartererParentName,
					charterer.Notes,
					charterer.[Type],
					charterer.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_ChartererParent charterer with (nolock)
				where
					charterer.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_ChartererParent with (tablock)
			set
				ChartererParentRlsKey = charterer.ChartererParentRlsKey,
				ChartererParentName = charterer.ChartererParentName,
				Notes = charterer.Notes,
				[Type] = charterer.[Type],
				Type1HashValue = [Charterer].Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_ChartererParent charterer with (nolock)
			where
				charterer.RecordStatus & @ExistingRecord = @ExistingRecord
				and charterer.ChartererParentAlternateKey = Warehouse.Dim_ChartererParent.ChartererParentAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_ChartererParent with (tablock)
			where
				not exists	(
								select
										1
									from
										ChartererParents cp with (nolock)
									where
										cp.QBRecId = ChartererParentAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_ChartererParent where ChartererParentKey < 0)
		begin
			delete
					Warehouse.Dim_ChartererParent with (tablock)
				where
					ChartererParentKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_ChartererParent on;
			insert
					Warehouse.Dim_ChartererParent with (tablock)	(
																		ChartererParentKey,
																		ChartererParentAlternateKey,
																		ChartererParentRlsKey,
																		ChartererParentName,
																		Notes,
																		[Type],
																		Type1HashValue,
																		RowCreatedDate,
																		RowUpdatedDate,
																		IsCurrentRow
																	)

				values	(
							-1,				-- ChartererParentKey
							-1,				-- ChartererParentAlternateKey
							'Unknown',		-- ChartererParentRlsKey
							'Unknown',		-- ChartererParentName
							'Unknown',		-- Notes
							'Unknown',		-- [Type]
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_ChartererParent off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end