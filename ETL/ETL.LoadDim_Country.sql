set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Country;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/12/2019
Description:	Creates the LoadDim_Country stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadDim_Country
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Country', 'U') is not null
		truncate table Staging.Dim_Country;

	begin try
		insert
				Staging.Dim_Country
		select
				country.QBRecId,
				country.CountryName,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				Country country
					left join	(
									select
											@ExistingRecord RecordStatus,
											CountryAlternateKey
										from
											Warehouse.Dim_Country
								) rs
						on rs.CountryAlternateKey = country.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging Country records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Country
			set
				-- Type 1 SCD
				Type1HashValue = hashbytes('MD2', CountryName);
		
		update
				Staging.Dim_Country
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Country wb
			where
				wb.CountryAlternateKey = Staging.Dim_Country.CountryAlternateKey
				and wb.Type1HashValue <> Staging.Dim_Country.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Countrys into Warehouse table
	begin try
		insert
				Warehouse.Dim_Country
			select
					country.CountryAlternateKey,
					country.CountryName,
					country.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Country country
				where
					country.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Country
			set
				CountryName = country.CountryName,
				Type1HashValue = country.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Country country
			where
				Country.RecordStatus & @ExistingRecord = @ExistingRecord
				and Country.CountryAlternateKey = Warehouse.Dim_Country.CountryAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Country
			where
				not exists	(
								select
										1
									from
										Country b
									where
										b.QBRecId = CountryAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Country where CountryKey = -1)
		begin
			delete
					Warehouse.Dim_Country
				where
					CountryKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Country on;
			insert
					Warehouse.Dim_Country	(
														CountryKey,
														CountryAlternateKey,
														CountryName,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,				-- CountryKey
							0,				-- CountryAlternateKey
							'Unknown',		-- CountryName
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Country off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end