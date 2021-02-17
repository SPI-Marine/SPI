set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Charterer;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the stored procedure LoadDim_Charterer
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/13/2020	Source data from FullStyles table
Brian Boswick	12/16/2020	Added ChartererParentAlternateKey for RLS
==========================================================================================================	
*/

create procedure ETL.LoadDim_Charterer
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Charterer', 'U') is not null
		truncate table Staging.Dim_Charterer;

	begin try
		insert
				Staging.Dim_Charterer with (tablock)
		select
				fs.QBRecId,
				fs.RelatedChartererParentID,
				'c_' + convert(varchar(50), fs.QBRecId) ChartererRlsKey,
				'cp_' + convert(varchar(50), fs.RelatedChartererParentID) ChartererParentRlsKey,
				fs.FullStyleName,
				charterer.ChartererParentName,
				fs.[Type],
				fs.[Address],
				fs.GroupNameFS,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				FullStyles fs (nolock)
					left join ChartererParents charterer with (nolock)
						on fs.RelatedChartererParentID = charterer.QBRecId
					left join	(
									select
											@ExistingRecord RecordStatus,
											ChartererAlternateKey
										from
											Warehouse.Dim_Charterer with (nolock)
								) rs
						on rs.ChartererAlternateKey = fs.QBRecId
			where
				fs.[Type] = 'Charterer';
	end try
	begin catch
		select @ErrorMsg = 'Staging Charterer records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Charterer with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																ChartererRlsKey,
																ChartererParentRlsKey,
																FullStyleName,
																ChartererParentName,
																[Type],
																[Address],
																GroupName
															)
												);
		
		update
				Staging.Dim_Charterer with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Charterer wc with (nolock)
			where
				wc.ChartererAlternateKey = Staging.Dim_Charterer.ChartererAlternateKey
				and wc.Type1HashValue <> Staging.Dim_Charterer.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Charterers into Warehouse table
	begin try
		insert
				Warehouse.Dim_Charterer  with (tablock)
			select
					charterer.ChartererAlternateKey,
					charterer.ChartererRlsKey,
					charterer.ChartererParentAlternateKey,
					charterer.ChartererParentRlsKey,
					charterer.FullStyleName,
					charterer.ChartererParentName,
					charterer.[Type],
					charterer.[Address],
					charterer.GroupName,
					charterer.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Charterer charterer with (nolock)
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
				Warehouse.Dim_Charterer with (tablock)
			set
				ChartererRlsKey = charterer.ChartererRlsKey,
				ChartererParentAlternateKey = charterer.ChartererParentAlternateKey,
				ChartererParentRlsKey = charterer.ChartererParentRlsKey,
				FullStyleName = charterer.FullStyleName,
				ChartererParentName = charterer.ChartererParentName,
				[Type] = charterer.[Type],
				[Address] = charterer.[Address],
				GroupName = charterer.GroupName,
				Type1HashValue = [Charterer].Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Charterer charterer with (nolock)
			where
				charterer.RecordStatus & @ExistingRecord = @ExistingRecord
				and charterer.ChartererAlternateKey = Warehouse.Dim_Charterer.ChartererAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Charterer with (tablock)
			where
				not exists	(
								select
										1
									from
										FullStyles fs with (nolock)
									where
										fs.QBRecId = ChartererAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Charterer where ChartererKey < 0)
		begin
			delete
					Warehouse.Dim_Charterer with (tablock)
				where
					ChartererKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Charterer on;
			insert
					Warehouse.Dim_Charterer with (tablock)	(
																ChartererKey,
																ChartererAlternateKey,
																ChartererRlsKey,
																ChartererParentAlternateKey,
																ChartererParentRlsKey,
																FullStyleName,
																ChartererParentName,
																[Type],
																[Address],
																GroupName,
																Type1HashValue,
																RowCreatedDate,
																RowUpdatedDate,
																IsCurrentRow
															)

				values	(
							-1,				-- ChartererKey
							-1,				-- ChartererAlternateKey
							'Unknown',		-- ChartererRlsKey
							-1,				-- ChartererParentAlternateKey
							'Unknown',		-- ChartererParentRlsKey
							'Unknown',		-- FullStyleName
							'Unknown',		-- ChartererParentName
							'Unknown',		-- [Type]
							'Unknown',		-- [Address]
							'Unknown',		-- GroupName
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Charterer off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end