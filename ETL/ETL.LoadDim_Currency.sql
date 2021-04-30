drop procedure if exists ETL.LoadDim_Currency;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/30/2021
Description:	Creates the LoadDim_Currency stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

create procedure ETL.LoadDim_Currency
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Currency', 'U') is not null
		truncate table Staging.Dim_Currency;

	begin try
		insert
				Staging.Dim_Currency
		select
			distinct
				cur.CurrencyCode								CurrencyCode,
				cur.CurrencyName								CurrencyName,
				0												Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord)				RecordStatus
			from
				HistoricalCurrencies cur
					left join	(
									select
											@ExistingRecord RecordStatus,
											CurrencyCode
										from
											Warehouse.Dim_Currency
								) rs
						on rs.CurrencyCode = cur.CurrencyCode;
	end try
	begin catch
		select @ErrorMsg = 'Staging Currency records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Currency
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																CurrencyCode,
																CurrencyName
															)
												);
		
		update
				Staging.Dim_Currency
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Currency wc
			where
				wc.CurrencyCode = Staging.Dim_Currency.CurrencyCode
				and wc.Type1HashValue <> Staging.Dim_Currency.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Currencies into Warehouse table
	begin try
		insert
				Warehouse.Dim_Currency
			select
					cur.CurrencyCode,
					cur.CurrencyName,
					cur.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Currency cur
				where
					cur.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Currency
			set
				CurrencyCode = cur.CurrencyCode,
				CurrencyName = cur.CurrencyName,
				Type1HashValue = cur.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Currency cur
			where
				cur.RecordStatus & @ExistingRecord = @ExistingRecord
				and cur.CurrencyCode = Warehouse.Dim_Currency.CurrencyCode
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Currency
			where
				not exists	(
								select
										1
									from
										HistoricalCurrencies hc
									where
										hc.CurrencyCode = Warehouse.Dim_Currency.CurrencyCode
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Currency where CurrencyKey = -1)
		begin
			delete
					Warehouse.Dim_Currency
				where
					CurrencyKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Currency on;
			insert
					Warehouse.Dim_Currency	(
														CurrencyKey,
														CurrencyCode,
														CurrencyName,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,				-- CurrencyKey
							0,				-- CurrencyCode
							'Unknown',		-- CurrencyName
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Currency off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end