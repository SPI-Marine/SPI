/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/28/2020
Description:	Creates the LoadDim_COA stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/10/2020	Added Charterer/Owner info
==========================================================================================================	
*/

set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_COA;
go

create procedure ETL.LoadDim_COA
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_COA', 'U') is not null
		truncate table Staging.Dim_COA;

	begin try
		insert
				Staging.Dim_COA with (tablock)
		select
				coa.RecordID,
				coa.COA_Title_Admin,
				coa.AddressCommission,
				coa.BrokerCommission,
				coa.[Status],
				coa.[P&C],
				coa.SPICOADate,
				coa.AddendumDate,
				coa.AddendumExpiryDate,
				coa.AddendumCommencementDate,
				coa.RenewalDate_DeclareBy,
				coa.ContractCancelling,
				coa.ContractCancelling,
				chartparent.ChartererParentName,
				ownerparent.OwnerParentName,
				chartfs.FullStyleName ChartererFullStyle,
				ownerfs.FullStyleName OwnerFullStyle,
				reg.[Name]	BrokerRegion,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				SPICOA coa with (nolock)
					left join SPIRegions reg (nolock)
						on reg.QBRecId = coa.RelatedSPIRegionID_ADMIN
					left join FullStyles ownerfs
						on ownerfs.QBRecId = coa.RelatedOwnerFullStyleID
					left join FullStyles chartfs
						on chartfs.QBRecId = coa.RelatedChartererFullStyleID
					left join OwnerParents ownerparent
						on ownerparent.QBRecId = ownerfs.RelatedOwnerParentId
					left join ChartererParents chartparent
						on chartparent.QBRecId = chartfs.RelatedChartererParentID
					left join	(
									select
											@ExistingRecord RecordStatus,
											COAAlternateKey
										from
											Warehouse.Dim_COA with (tablock)
								) rs
						on coa.RecordID = rs.COAAlternateKey;
	end try
	begin catch
		select @ErrorMsg = 'Staging COA records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_COA with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																COATitle,
																AddressCommission,
																BrokerCommission,
																[Status],
																[P&C],
																COADate,
																AddendumDate,
																AddendumExpiryDate,
																AddendumCommencementDate,
																RenewalDeclareByDate,
																ContractCommencementDate,
																ContractCancellingDate,
																ChartererParent,
																OwnerParent,
																ChartererFullStyle,
																OwnerFullStyle,
																BrokerRegion
															)
												);
		
		update
				Staging.Dim_COA with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_COA wp with (nolock)
			where
				wp.COAAlternateKey = Staging.Dim_COA.COAAlternateKey
				and wp.Type1HashValue <> Staging.Dim_COA.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new COAs into Warehouse table
	begin try
		insert
				Warehouse.Dim_COA with (tablock)
			select
					coa.COAAlternateKey,
					coa.COATitle,
					coa.AddressCommission,
					coa.BrokerCommission,
					coa.[Status],
					coa.[P&C],
					coa.COADate,
					coa.AddendumDate,
					coa.AddendumExpiryDate,
					coa.AddendumCommencementDate,
					coa.RenewalDeclareByDate,
					coa.ContractCommencementDate,
					coa.ContractCancellingDate,
					coa.ChartererParent,
					coa.OwnerParent,
					coa.ChartererFullStyle,
					coa.OwnerFullStyle,
					coa.BrokerRegion,
					coa.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_COA coa with (nolock)
				where
					coa.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_COA with (tablock)
			set
				COATitle = coa.COATitle,
				AddressCommission = coa.AddressCommission,
				BrokerCommission = coa.BrokerCommission,
				[Status] = coa.[Status],
				[P&C] = coa.[P&C],
				COADate = coa.COADate,
				AddendumDate = coa.AddendumDate,
				AddendumExpiryDate = coa.AddendumExpiryDate,
				AddendumCommencementDate = coa.AddendumCommencementDate,
				RenewalDeclareByDate = coa.RenewalDeclareByDate,
				ContractCommencementDate = coa.ContractCommencementDate,
				ContractCancellingDate = coa.ContractCancellingDate,
				ChartererParent = coa.ChartererParent,
				OwnerParent = coa.OwnerParent,
				ChartererFullStyle = coa.ChartererFullStyle,
				OwnerFullStyle = coa.OwnerFullStyle,
				BrokerRegion = coa.BrokerRegion,
				Type1HashValue = coa.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_COA coa with (nolock)
			where
				coa.RecordStatus & @ExistingRecord = @ExistingRecord
				and coa.COAAlternateKey = Warehouse.Dim_COA.COAAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_COA with (tablock)
			where
				not exists	(
								select
										1
									from
										SPICOA coa with (nolock)
									where
										coa.RecordID = COAAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_COA where COAKey < 0)
		begin
			delete
					Warehouse.Dim_COA with (tablock)
				where
					COAKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_COA on;
			insert
					Warehouse.Dim_COA with (tablock)	(
															COAKey,
															COAAlternateKey,
															COATitle,
															AddressCommission,
															BrokerCommission,
															[Status],
															[P&C],
															COADate,
															AddendumDate,
															AddendumExpiryDate,
															AddendumCommencementDate,
															RenewalDeclareByDate,
															ContractCommencementDate,
															ContractCancellingDate,
															ChartererParent,
															OwnerParent,
															ChartererFullStyle,
															OwnerFullStyle,
															BrokerRegion,
															Type1HashValue,
															RowCreatedDate,
															RowUpdatedDate,
															IsCurrentRow
														)

				values	(
							-1,				-- COAKey
							-1,				-- COAAlternateKey
							'Unknown',		-- COATitle
							0.0,			-- AddressCommission,
							0.0,			-- BrokerCommission,
							'Unknown',		-- [Status],
							'Unknown',		-- [P&C],
							'12/30/1899',	-- COADate,
							'12/30/1899',	-- AddendumDate,
							'12/30/1899',	-- AddendumExpiryDate,
							'12/30/1899',	-- AddendumCommencementDate,
							'12/30/1899',	-- RenewalDeclareByDate,
							'12/30/1899',	-- ContractCommencementDate,
							'12/30/1899',	-- ContractCancellingDate,
							'Unknown',		-- ChartererParent
							'Unknown',		-- OwnerParent
							'Unknown',		-- ChartererFullStyle
							'Unknown',		-- OwnerFullStyle
							'Unknown',		-- OwnerFullStyle
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_COA off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end