/*
==========================================================================================================
Author:			Brian Boswick
Create date:	03/14/2019
Description:	Creates the LoadDim_PortBerth stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	05/20/2019	Remove deleted records from Warehouse
Brian Boswick	01/31/2020	Added Area and Region ETL logic
Brian Boswick	05/27/2021	Removed City, StateRegion
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_PortBerth;
go

create procedure ETL.LoadDim_PortBerth
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_PortBerth', 'U') is not null
		truncate table Staging.Dim_PortBerth;

	begin try
		with UniquePortBerths(PortAlternateKey, BerthAlternateKey)
		as
		(
			select
				distinct	
					pp.RelatedPortId,
					pb.RelatedBerthId
				from
					ParcelBerths pb
						join ParcelPorts pp
							on pp.QBRecId = pb.RelatedLDPId
				where
					pp.RelatedPortId is not null
					and pb.RelatedBerthId is not null
		)

		insert
				Staging.Dim_PortBerth
		select
			distinct
				portberth.PortAlternateKey						PortAlternateKey,
				portberth.BerthAlternateKey						BerthAlternateKey,
				concat	(
							isnull([port].PortName, 'Unknown'),
							'/',
							isnull(berth.BerthName, 'Unknown')
						)										PortBerthName,
				isnull([port].PortName, 'Unknown')				PortName,
				isnull(berth.BerthName, 'Unknown')				BerthName,
				[port].Country									Country,
				[port].Comments									Comments,
				case 
					when right([port].Latitude, 1) = 's'
						then try_convert(numeric(10, 4), replace(replace([port].Latitude, ' ', '.'), 's', '')) * -1
					when right([port].Latitude, 1) = 'n'
						then try_convert(numeric(10, 4), replace(replace([port].Latitude, ' ', '.'), 'n', ''))
					else null
				end												Latitude,
				case 
					when right([port].Longitude, 1) = 'w'
						then try_convert(numeric(10, 4), replace(replace([port].Longitude, ' ', '.'), 'w', '')) * -1
					when right([port].Longitude, 1) = 'e'
						then try_convert(numeric(10, 4), replace(replace([port].Longitude, ' ', '.'), 'e', ''))
					else null
				end												Longitude,
				[port].PortCosts								PortCosts,
				area.[Name]										Area,
				region.RegionName								Region,
				0												Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord)				RecordStatus
			from
				UniquePortBerths portberth
					left join [Ports] [port]
						on [port].QBRecId = portberth.PortAlternateKey
					left join ShippingAreas area
						on area.QBRecId = [port].RelatedShippingAreaId
					left join ShippingRegions region
						on region.QBRecId = area.RelatedSARegionID
					left join Berths berth
						on berth.QBRecId = portberth.BerthAlternateKey
					left join	(
									select
											@ExistingRecord RecordStatus,
											PortAlternateKey,
											BerthAlternateKey
										from
											Warehouse.Dim_PortBerth
								) rs
						on rs.PortAlternateKey = portberth.PortAlternateKey
							and rs.BerthAlternateKey = portberth.BerthAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging PortBerth records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_PortBerth
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																PortBerthName,
																PortName,
																BerthName,
																Country,
																Comments,
																Latitude,
																Longitude,
																PortCosts,
																Area,
																Region
															)
												);
		
		update
				Staging.Dim_PortBerth
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_PortBerth wpb
			where
				wpb.PortAlternateKey = Staging.Dim_PortBerth.PortAlternateKey
				and wpb.BerthAlternateKey = Staging.Dim_PortBerth.BerthAlternateKey
				and wpb.Type1HashValue <> Staging.Dim_PortBerth.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new ports into Warehouse table
	begin try
		insert
				Warehouse.Dim_PortBerth
			select
					portberth.PortAlternateKey,
					portberth.BerthAlternateKey,
					portberth.PortBerthName,
					portberth.PortName,
					portberth.BerthName,
					portberth.Country,
					portberth.Comments,
					portberth.Latitude,
					portberth.Longitude,
					portberth.PortCosts,
					portberth.Area,
					portberth.Region,
					portberth.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_PortBerth portberth
				where
					portberth.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_PortBerth
			set
				PortBerthName = portberth.PortBerthName,
				PortName = portberth.PortName,
				BerthName = portberth.BerthName,
				Country = portberth.Country,
				Comments = portberth.Comments,
				Latitude = portberth.Latitude,
				Longitude = portberth.Longitude,
				PortCosts = portberth.PortCosts,
				Area = portberth.Area,
				Region = portberth.Region,
				Type1HashValue = portberth.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_PortBerth portberth
			where
				portberth.RecordStatus & @ExistingRecord = @ExistingRecord
				and portberth.PortAlternateKey = Warehouse.Dim_PortBerth.PortAlternateKey
				and portberth.BerthAlternateKey = Warehouse.Dim_PortBerth.BerthAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_PortBerth
			where
				not exists	(
								select
										1
									from
										Berths b
									where
										b.QBRecId = BerthAlternateKey
							)
				and not exists	(
									select
											1
										from
											[Ports] p
										where
											p.QBRecId = PortAlternateKey
								);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_PortBerth where PortBerthKey = -1)
		begin
			delete
					Warehouse.Dim_PortBerth
				where
					PortBerthKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_PortBerth on;
			insert
					Warehouse.Dim_PortBerth	(
														PortBerthKey,
														PortAlternateKey,
														BerthAlternateKey,
														PortBerthName,
														PortName,
														BerthName,
														Country,
														Comments,
														Latitude,
														Longitude,
														PortCosts,
														Area,
														Region,
														Type1HashValue,
														RowCreatedDate,
														RowUpdatedDate,
														IsCurrentRow
													)

				values	(
							-1,				-- PortBerthKey
							0,				-- PortAlternateKey
							0,				-- BerthAlternateKey
							'Unknown',		-- PortBerthName
							'Unknown',		-- PortName
							'Unknown',		-- BerthName
							'Unknown',		-- Country
							'Unknown',		-- Comments
							0,				-- Latitude
							0,				-- Longitude
							'Unknown',		-- PortCosts
							'Unknown',		-- Area
							'Unknown',		-- Region
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_PortBerth off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end