set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_COA;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/28/2020
Description:	Creates the LoadDim_COA stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/10/2020	Added Charterer/Owner info
Brian Boswick	07/09/2021	Changed granularity to the Trade Lane level and added new fields
==========================================================================================================	
*/

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
				tl.RecordID								TradeLaneAlternateKey,
				coa.RecordID							COAAlternateKey,
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
				chartfs.FullStyleName					ChartererFullStyle,
				ownerfs.FullStyleName					OwnerFullStyle,
				reg.[Name]								BrokerRegion,
				coa.StatusFormula_ADMIN,
				coa.RenewalStatus,
				coa.ContractNameType,
				coa.COAorServiceAgreement,
				coa.[Broker],
				coa.OpsPICFullNameTM,
				coa.SPIOffice,
				coa.CPForm,
				coa.FreightBasis_Entry,
				coa.[Period],
				coa.Mos_Years,
				coa.[Option],
				coa.TermsforOptionalPeriodAsia,
				coa.DemurrageTimeBar,
				coa.DemOutcome1,
				coa.DemurrageCOANotes,
				tl.TradelaneNumLiftingsMin_Entry,
				tl.TradelaneNumLiftingsMax_Entry,
				tl.TradeLaneLiftingQtyMin_Entry,
				tl.TradeLaneLiftingQtyMax_Entry,
				tl.LoadOption,
				tl.FreightDetails,
				tl.LiftingRequirementOptions,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord)		RecordStatus
			from
				TradeLane tl
					left join SPICOA coa with (nolock)
						on tl.RelatedSPICOAID = coa.RecordID
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
											TradeLaneAlternateKey
										from
											Warehouse.Dim_COA with (tablock)
								) rs
						on coa.RecordID = rs.TradeLaneAlternateKey;
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
																convert(varchar(50), COAAlternateKey),
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
																StatusFormula,
																RenewalStatus,
																ContractNameType,
																COAorServiceAgreement,
																[Broker],
																OpsPICFullNameTM,
																SPIOffice,
																CPForm,
																FreightBasisEntry,
																[Period],
																Mos_Years,
																[Option],
																TermsforOptionalPeriodAsia,
																DemurrageTimeBar,
																DemOutcome1,
																DemurrageCOANotes,
																convert(varchar(50), TradelaneNumLiftingsMinEntry),
																convert(varchar(50), TradelaneNumLiftingsMaxEntry),
																convert(varchar(50), TradeLaneLiftingQtyMinEntry),
																convert(varchar(50), TradeLaneLiftingQtyMaxEntry),
																LoadOption,
																FreightDetails,
																LiftingRequirementOptions
															)
												);
		
		update
				Staging.Dim_COA with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_COA wp with (nolock)
			where
				wp.TradeLaneAlternateKey = Staging.Dim_COA.TradeLaneAlternateKey
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
					coa.TradeLaneAlternateKey,
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
					'Y' IsCurrentRow,
					coa.StatusFormula,
					coa.RenewalStatus,
					coa.ContractNameType,
					coa.COAorServiceAgreement,
					coa.[Broker],
					coa.OpsPICFullNameTM,
					coa.SPIOffice,
					coa.CPForm,
					coa.FreightBasisEntry,
					coa.[Period],
					coa.Mos_Years,
					coa.[Option],
					coa.TermsforOptionalPeriodAsia,
					coa.DemurrageTimeBar,
					coa.DemOutcome1,
					coa.DemurrageCOANotes,
					coa.TradelaneNumLiftingsMinEntry,
					coa.TradelaneNumLiftingsMaxEntry,
					coa.TradeLaneLiftingQtyMinEntry,
					coa.TradeLaneLiftingQtyMaxEntry,
					coa.LoadOption,
					coa.FreightDetails,
					coa.LiftingRequirementOptions
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
				COAAlternateKey = coa.COAAlternateKey,
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
				StatusFormula = coa.StatusFormula,
				RenewalStatus= coa.RenewalStatus,
				ContractNameType = coa.ContractNameType,
				COAorServiceAgreement = coa.COAorServiceAgreement,
				[Broker] = coa.[Broker],
				OpsPICFullNameTM = coa.OpsPICFullNameTM,
				SPIOffice = coa.SPIOffice,
				CPForm = coa.CPForm,
				FreightBasisEntry = coa.FreightBasisEntry,
				[Period] = coa.[Period],
				Mos_Years = coa.Mos_Years,
				[Option] = coa.[Option],
				TermsforOptionalPeriodAsia = coa.TermsforOptionalPeriodAsia,
				DemurrageTimeBar = coa.DemurrageTimeBar,
				DemOutcome1 = coa.DemOutcome1,
				DemurrageCOANotes = coa.DemurrageCOANotes,
				TradelaneNumLiftingsMinEntry = coa.TradelaneNumLiftingsMinEntry,
				TradelaneNumLiftingsMaxEntry = coa.TradelaneNumLiftingsMaxEntry,
				TradeLaneLiftingQtyMinEntry = coa.TradeLaneLiftingQtyMinEntry,
				TradeLaneLiftingQtyMaxEntry = coa.TradeLaneLiftingQtyMaxEntry,
				LoadOption = coa.LoadOption,
				FreightDetails = coa.FreightDetails,
				LiftingRequirementOptions = coa.LiftingRequirementOptions,
				Type1HashValue = coa.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_COA coa with (nolock)
			where
				coa.RecordStatus & @ExistingRecord = @ExistingRecord
				and coa.TradeLaneAlternateKey = Warehouse.Dim_COA.TradeLaneAlternateKey
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
										TradeLane tl with (nolock)
									where
										tl.RecordID = TradeLaneAlternateKey
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
															TradeLaneAlternateKey,
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
															IsCurrentRow,
															StatusFormula,
															RenewalStatus,
															ContractNameType,
															COAorServiceAgreement,
															[Broker],
															OpsPICFullNameTM,
															SPIOffice,
															CPForm,
															FreightBasisEntry,
															[Period],
															Mos_Years,
															[Option],
															TermsforOptionalPeriodAsia,
															DemurrageTimeBar,
															DemOutcome1,
															DemurrageCOANotes,
															TradelaneNumLiftingsMinEntry,
															TradelaneNumLiftingsMaxEntry,
															TradeLaneLiftingQtyMinEntry,
															TradeLaneLiftingQtyMaxEntry,
															LoadOption,
															FreightDetails,
															LiftingRequirementOptions
														)

				values	(
							-1,				-- COAKey
							-1,				-- TradeLaneAlternateKey
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
							'Unknown',		-- BrokerRegion,
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y',			-- IsCurrentRow
							'Unknown',		-- StatusFormula,
							'Unknown',		-- RenewalStatus,
							'Unknown',		-- ContractNameType,
							'Unknown',		-- COAorServiceAgreement,
							'Unknown',		-- [Broker],
							'Unknown',		-- OpsPICFullNameTM,
							'Unknown',		-- SPIOffice,
							'Unknown',		-- CPForm,
							'Unknown',		-- FreightBasisEntry,
							'Unknown',		-- [Period],
							'Unknown',		-- Mos_Years,
							'Unknown',		-- [Option],
							'Unknown',		-- TermsforOptionalPeriodAsia,
							'Unknown',		-- DemurrageTimeBar,
							'Unknown',		-- DemOutcome1,
							'Unknown',		-- DemurrageCOANotes,
							-1,				-- TradelaneNumLiftingsMinEntry,
							-1,				-- TradelaneNumLiftingsMaxEntry,
							-1,				-- TradeLaneLiftingQtyMinEntry,
							-1,				-- TradeLaneLiftingQtyMaxEntry,
							'Unknown',		-- LoadOption,
							'Unknown',		-- FreightDetails,
							'Unknown'		-- LiftingRequirementOptions
						);
			set identity_insert Warehouse.Dim_COA off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end