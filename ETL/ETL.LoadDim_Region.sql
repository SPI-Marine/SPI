set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_Region;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/09/2020
Description:	Creates the LoadDim_Region stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	01/07/2021	Added RegionRlsKey ETL
==========================================================================================================	
*/

create procedure ETL.LoadDim_Region
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_Region', 'U') is not null
		truncate table Staging.Dim_Region;

	begin try
		insert
				Staging.Dim_Region
		select
				Region.QBRecId,
				'r_' + convert(varchar(50), Region.QBRecId) RegionRlsKey,
				Region.RegionName,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				ShippingRegions Region
					left join	(
									select
											@ExistingRecord RecordStatus,
											RegionAlternateKey
										from
											Warehouse.Dim_Region
								) rs
						on rs.RegionAlternateKey = Region.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging Region records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_Region
			set
				-- Type 1 SCD
				Type1HashValue = hashbytes('MD2', concat(RegionName, RegionRlsKey));
		
		update
				Staging.Dim_Region
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_Region wr
			where
				wr.RegionAlternateKey = Staging.Dim_Region.RegionAlternateKey
				and wr.Type1HashValue <> Staging.Dim_Region.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new Regions into Warehouse table
	begin try
		insert
				Warehouse.Dim_Region
			select
					Region.RegionAlternateKey,
					Region.RegionRlsKey,
					Region.RegionName,
					Region.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_Region Region
				where
					Region.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_Region
			set
				RegionName = Region.RegionName,
				RegionRlsKey = Region.RegionRlsKey,
				Type1HashValue = Region.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_Region Region
			where
				Region.RecordStatus & @ExistingRecord = @ExistingRecord
				and Region.RegionAlternateKey = Warehouse.Dim_Region.RegionAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_Region
			where
				not exists	(
								select
										1
									from
										ShippingRegions r
									where
										r.QBRecId = RegionAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_Region where RegionKey = -1)
		begin
			delete
					Warehouse.Dim_Region
				where
					RegionKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_Region on;
			insert
					Warehouse.Dim_Region	(
														RegionKey,
														RegionAlternateKey,
														RegionRlsKey,
														RegionName,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,				-- RegionKey
							0,				-- RegionAlternateKey
							'Unknown',		-- RegionRlsKey
							'Unknown',		-- RegionName
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_Region off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end