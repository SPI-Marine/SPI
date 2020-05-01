set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_PostFixture;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/28/2018
Description:	Creates the LoadDim_PostFixture stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	04/20/2019	Added ETL for COA related information
Brian Boswick	04/25/2019	Added LaycanCancellingOriginal, LaycanCancellingFinal_QBC,
							LaycanCommencementFinal_QBC,
Brian Boswick	07/01/2019	Added four new fields from QB
Brian Boswick	07/17/2019	Added OwnerParent and ChartererParent fields
Brian Boswick	02/19/2020	Added COA_Title_Admin ETL logic
==========================================================================================================	
*/

create procedure ETL.LoadDim_PostFixture
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_PostFixture', 'U') is not null
		truncate table Staging.Dim_PostFixture;

	begin try
		insert
				Staging.Dim_PostFixture with (tablock)
		select
			distinct
				fixture.QBRecId,
				fixture.RelatedBroker						BrokerEmail,
				isnull(brokername.FirstName, 'Unknown')		BrokerFirstName,
				isnull(brokername.LastName, 'Unknown')		BrokerLastName,
				concat	(
							isnull(brokername.LastName, 'Unknown'),
							', ',
							isnull(brokername.FirstName, 'Unknown')
						)									BrokerFullName,
				ownerfullstyle.FullStyleName				OwnerFullStyle,
				chartererfullstyle.FullStyleName			ChartererFullStyle,
				ownerparent.OwnerParentName					OwnerParentName,
				chartererparent.ChartererParentName			ChartererParentName,
				fixture.RelatedOpsPrimary,
				fixture.RelatedOpsBackup,
				fixture.CPDate,
				fixture.CPForm,
				fixture.DemurrageRate,
				fixture.TimeBar,
				fixture.AddressCommissionPercent,
				fixture.BrokerCommissionPercent,
				fixture.LaytimeAllowedLoad,
				fixture.LaytimeAllowedDisch,
				fixture.ShincReversible,
				fixture.VesselNameSnap,
				fixture.DemurrageAmountAgreed,
				fixture.CharterInvoiced,
				fixture.PaymentType,
				fixture.FreightLumpSumEntry,
				fixture.DischFAC,
				fixture.LaytimeOption,
				fixture.OwnersRef,
				fixture.CharterersRef,
				fixture.CurrencyInvoice,
				fixture.CharteringPicSnap,
				fixture.OperationsPicSnap,
				fixture.BrokerCommDemurrage,
				fixture.AddCommDeadFreight,
				fixture.DemurrageClaimReceived,
				fixture.VoyageNumber,
				fixture.LaycanToBeAmended,
				fixture.LaycanCancellingAmended,
				fixture.LaycanCommencementAmended,
				fixture.CurrencyCP,
				fixture.FixtureStatus,
				fixture.LaytimeAllowedTotalLoad,
				fixture.LaytimeAllowedTotalDisch,
				fixture.FrtRatePmt,
				fixture.BrokerFrtComm,
				fixture.P2FixtureRefNum,
				fixture.VesselFixedOfficial,
				fixture.LaycanCommencementOriginal,
				coa.RecordID,
				coa.[Status],
				coa.SPICOADate,
				coa.AddendumDate,
				coa.AddendumExpiryDate,
				coa.AddendumCommencementDate,
				coa.RenewalDate_DeclareBy,
				coa.ContractCommencement,
				coa.ContractCancelling,
				coa.COA_Title_Admin,
				fixture.LaycanCancelOrig,
				fixture.Laycan_Cancelling_Final_QBC,
				fixture.Laycan_Commencement_Final_QBC,
				fixture.SPI_Fixture_Status,
				region.RegionName,
				fixture.LAF_Disch_Mtph_QBC,
				fixture.LAF_Load_Mtph_QBC,
				fixture.LAF_Total_hrs_QBC,
				fixture.LaytimeAllowedTypeFixture_QBC,
				fixture.FixtureType,
				chartererfullstyle.GroupNameFS GroupName,
				office.OfficeName SPIOffice,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				PostFixtures fixture
					left join FullStyles ownerfullstyle
						on fixture.RelatedOwnerFullStyle = ownerfullstyle.QBRecId
					left join OwnerParents ownerparent
						on ownerparent.QBRecId = ownerfullstyle.RelatedOwnerParentId
					left join FullStyles chartererfullstyle
						on chartererfullstyle.QBRecId = fixture.RelatedChartererFullStyle
					left join ChartererParents chartererparent
						on chartererparent.QBRecId = chartererfullstyle.RelatedChartererParentId
					left join TeamMembers brokername
						on rtrim(ltrim(brokername.EmailAddress)) = rtrim(ltrim(fixture.RelatedBroker))
					left join SPICOA coa
						on coa.RecordID = fixture.RelatedSPICOAId
					left join SPIOffices office
						on office.QBRecId = fixture.RelatedSPIOfficeID
					left join	(
									select
											tm.EmailAddress,
											r.[Name] RegionName
										from
											TeamMembers tm
												join SpiOffices o
													on o.QBRecId = tm.RelatedSPIOfficeId
												join SPIRegions r
													on r.QBRecId = o.RelatedSpiRegionId
								) region
						on region.EmailAddress = fixture.RelatedBroker
					left join	(
									select
											@ExistingRecord RecordStatus,
											PostFixtureAlternateKey
										from
											Warehouse.Dim_PostFixture
								) rs
						on rs.PostFixtureAlternateKey = fixture.QBRecId;
	end try
	begin catch
		select @ErrorMsg = 'Staging PostFixture records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_PostFixture with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																BrokerEmail,
																BrokerFirstName,
																BrokerLastName,
																BrokerFullName,
																OwnerFullStyle,
																ChartererFullStyle,
																OwnerParent,
																ChartererParent,
																RelatedOpsPrimary,
																RelatedOpsBackup,
																convert(nvarchar(30), CPDate),
																CPForm,
																convert(nvarchar(30), DemurrageRate),
																convert(nvarchar(30), TimeBar),
																convert(nvarchar(30), AddressCommissionPercent),
																convert(nvarchar(30), BrokerCommissionPercent),
																convert(nvarchar(30), LaytimeAllowedLoad),
																convert(nvarchar(30), LaytimeAllowedDisch),
																ShincReversible,
																VesselNameSnap,
																convert(nvarchar(30), DemurrageAmountAgreed),
																CharterInvoiced,
																PaymentType,
																convert(nvarchar(30), FreightLumpSumEntry),
																DischargeFAC,
																LaytimeOption,
																OwnersReference,
																CharterersReference,
																CurrencyInvoice,
																CharteringPicSnap,
																OperationsPicSnap,
																BrokerCommDemurrage,
																AddCommDeadFreight,
																DemurrageClaimReceived,
																VoyageNumber,
																LaycanToBeAmended,
																LaycanCancellingAmended,
																LaycanCommencementAmended,
																CurrencyCP,
																FixtureStatus,
																convert(nvarchar(30), LaytimeAllowedTotalLoad),
																convert(nvarchar(30), LaytimeAllowedTotalDisch),
																convert(nvarchar(30), FrtRatePmt),
																convert(nvarchar(30), BrokerFrtComm),
																P2FixtureRefNum,
																VesselFixedOfficial,
																LaycanCommencementOriginal,
																SPI_COA_Number,
																COA_Status,
																COA_Date,
																COA_AddendumDate,
																COA_AddendumExpiryDate,
																COA_AddendumCommencementDate,
																COA_RenewalDateDeclareBy,
																COA_ContractCommencement,
																COA_ContractCancelling,
																COA_Title_Admin,
																LaycanCancellingOriginal,
																LaycanCancellingFinal_QBC,
																LaycanCommencementFinal_QBC,
																SPIFixtureStatus,
																Region,
																LAF_Disch_Mtph_QBC,
																LAF_Load_Mtph_QBC,
																LAF_Total_hrs_QBC,
																LaytimeAllowedTypeFixture_QBC,
																FixtureType,
																GroupName,
																SPIOffice
															)
												);
		
		update
				Staging.Dim_PostFixture with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_PostFixture wpf
			where
				wpf.PostFixtureAlternateKey = Staging.Dim_PostFixture.PostFixtureAlternateKey
				and wpf.Type1HashValue <> Staging.Dim_PostFixture.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new post fixtures into Warehouse table
	begin try
		insert
				Warehouse.Dim_PostFixture with (tablock)
			select
					fixture.PostFixtureAlternateKey,
					fixture.BrokerEmail,
					fixture.BrokerFirstName,
					fixture.BrokerLastName,
					fixture.BrokerFullName,
					fixture.OwnerFullStyle,
					fixture.ChartererFullStyle,
					fixture.OwnerParent,
					fixture.ChartererParent,
					fixture.RelatedOpsPrimary,
					fixture.RelatedOpsBackup,
					fixture.CPDate,
					fixture.CPForm,
					fixture.DemurrageRate,
					fixture.TimeBar,
					fixture.AddressCommissionPercent,
					fixture.BrokerCommissionPercent,
					fixture.LaytimeAllowedLoad,
					fixture.LaytimeAllowedDisch,
					fixture.ShincReversible,
					fixture.VesselNameSnap,
					fixture.DemurrageAmountAgreed,
					fixture.CharterInvoiced,
					fixture.PaymentType,
					fixture.FreightLumpSumEntry,
					fixture.DischargeFAC,
					fixture.LaytimeOption,
					fixture.OwnersReference,
					fixture.CharterersReference,
					fixture.CurrencyInvoice,
					fixture.CharteringPicSnap,
					fixture.OperationsPicSnap,
					fixture.BrokerCommDemurrage,
					fixture.AddCommDeadFreight,
					fixture.DemurrageClaimReceived,
					fixture.VoyageNumber,
					fixture.LaycanToBeAmended,
					fixture.LaycanCancellingAmended,
					fixture.LaycanCommencementAmended,
					fixture.CurrencyCP,
					fixture.FixtureStatus,
					fixture.LaytimeAllowedTotalLoad,
					fixture.LaytimeAllowedTotalDisch,
					fixture.FrtRatePmt,
					fixture.BrokerFrtComm,
					fixture.P2FixtureRefNum,
					fixture.VesselFixedOfficial,
					fixture.LaycanCommencementOriginal,
					fixture.SPI_COA_Number,
					fixture.COA_Status,
					fixture.COA_Date,
					fixture.COA_AddendumDate,
					fixture.COA_AddendumExpiryDate,
					fixture.COA_AddendumCommencementDate,
					fixture.COA_RenewalDateDeclareBy,
					fixture.COA_ContractCommencement,
					fixture.COA_ContractCancelling,
					fixture.COA_Title_Admin,
					fixture.LaycanCancellingOriginal,
					fixture.LaycanCancellingFinal_QBC,
					fixture.LaycanCommencementFinal_QBC,
					fixture.SPIFixtureStatus,
					fixture.Region,
					fixture.LAF_Disch_Mtph_QBC,
					fixture.LAF_Load_Mtph_QBC,
					fixture.LAF_Total_hrs_QBC,
					fixture.LaytimeAllowedTypeFixture_QBC,
					fixture.FixtureType,
					fixture.GroupName,
					fixture.SPIOffice,
					fixture.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_PostFixture fixture
				where
					fixture.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_PostFixture with (tablock)
			set
				BrokerEmail = fixture.BrokerEmail,
				BrokerFirstName = fixture.BrokerFirstName,
				BrokerLastName = fixture.BrokerLastName,
				BrokerFullName = fixture.BrokerFullName,
				OwnerFullStyle = fixture.OwnerFullStyle,
				ChartererFullStyle = fixture.ChartererFullStyle,
				OwnerParent = fixture.OwnerParent,
				ChartererParent = fixture.ChartererParent,
				RelatedOpsPrimary = fixture.RelatedOpsPrimary,
				RelatedOpsBackup = fixture.RelatedOpsBackup,
				CPDate = fixture.CPDate,
				CPForm = fixture.CPForm,
				DemurrageRate = fixture.DemurrageRate,
				TimeBar = fixture.TimeBar,
				AddressCommissionPercent = fixture.AddressCommissionPercent,
				BrokerCommissionPercent = fixture.BrokerCommissionPercent,
				LaytimeAllowedLoad = fixture.LaytimeAllowedLoad,
				LaytimeAllowedDisch = fixture.LaytimeAllowedDisch,
				ShincReversible = fixture.ShincReversible,
				VesselNameSnap = fixture.VesselNameSnap,
				DemurrageAmountAgreed = fixture.DemurrageAmountAgreed,
				CharterInvoiced = fixture.CharterInvoiced,
				PaymentType = fixture.PaymentType,
				FreightLumpSumEntry = fixture.FreightLumpSumEntry,
				DischargeFAC = fixture.DischargeFAC,
				LaytimeOption = fixture.LaytimeOption,
				OwnersReference = fixture.OwnersReference,
				CharterersReference = fixture.CharterersReference,
				CurrencyInvoice = fixture.CurrencyInvoice,
				CharteringPicSnap = fixture.CharteringPicSnap,
				OperationsPicSnap = fixture.OperationsPicSnap,
				BrokerCommDemurrage = fixture.BrokerCommDemurrage,
				AddCommDeadFreight = fixture.AddCommDeadFreight,
				DemurrageClaimReceived = fixture.DemurrageClaimReceived,
				VoyageNumber = fixture.VoyageNumber,
				LaycanToBeAmended = fixture.LaycanToBeAmended,
				LaycanCancellingAmended = fixture.LaycanCancellingAmended,
				LaycanCommencementAmended = fixture.LaycanCommencementAmended,
				CurrencyCP = fixture.CurrencyCP,
				FixtureStatus = fixture.FixtureStatus,
				LaytimeAllowedTotalLoad = fixture.LaytimeAllowedTotalLoad,
				LaytimeAllowedTotalDisch = fixture.LaytimeAllowedTotalDisch,
				FrtRatePmt = fixture.FrtRatePmt,
				BrokerFrtComm = fixture.BrokerFrtComm,
				P2FixtureRefNum = fixture.P2FixtureRefNum,
				VesselFixedOfficial = fixture.VesselFixedOfficial,
				LaycanCommencementOriginal = fixture.LaycanCommencementOriginal,
				SPI_COA_Number = fixture.SPI_COA_Number,
				COA_Status = fixture.[COA_Status],
				COA_Date = fixture.COA_Date,
				COA_AddendumDate = fixture.COA_AddendumDate,
				COA_AddendumExpiryDate = fixture.COA_AddendumExpiryDate,
				COA_AddendumCommencementDate = fixture.COA_AddendumCommencementDate,
				COA_RenewalDateDeclareBy = fixture.COA_RenewalDateDeclareBy,
				COA_ContractCommencement = fixture.COA_ContractCommencement,
				COA_ContractCancelling = fixture.COA_ContractCancelling,
				COA_Title_Admin = fixture.COA_Title_Admin,
				LaycanCancellingOriginal = fixture.LaycanCancellingOriginal,
				LaycanCancellingFinal_QBC = fixture.LaycanCancellingFinal_QBC,
				LaycanCommencementFinal_QBC = fixture.LaycanCommencementFinal_QBC,
				SPIFixtureStatus = fixture.SPIFixtureStatus,
				Region = fixture.Region,
				LAF_Disch_Mtph_QBC = fixture.LAF_Disch_Mtph_QBC,
				LAF_Load_Mtph_QBC = fixture.LAF_Load_Mtph_QBC,
				LAF_Total_hrs_QBC = fixture.LAF_Total_hrs_QBC,
				LaytimeAllowedTypeFixture_QBC = fixture.LaytimeAllowedTypeFixture_QBC,
				FixtureType = fixture.FixtureType,
				GroupName = fixture.GroupName,
				SPIOffice = fixture.SPIOffice,
				Type1HashValue = fixture.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_PostFixture fixture
			where
				RecordStatus & @ExistingRecord = @ExistingRecord
				and fixture.PostFixtureAlternateKey = Warehouse.Dim_PostFixture.PostFixtureAlternateKey
				and RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_PostFixture
			where
				not exists	(
								select
										1
									from
										PostFixtures pf
									where
										pf.QBRecId = PostFixtureAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_PostFixture where PostFixtureKey = -1)
		begin
			delete
					Warehouse.Dim_PostFixture
				where
					PostFixtureKey = -1;
		end
		else
		begin
			set identity_insert Warehouse.Dim_PostFixture on;
			insert
					Warehouse.Dim_PostFixture	(
													PostFixtureKey,
													PostFixtureAlternateKey,
													BrokerEmail,
													BrokerFirstName,
													BrokerLastName,
													BrokerFullName,
													OwnerFullStyle,
													ChartererFullStyle,
													OwnerParent,
													ChartererParent,
													RelatedOpsPrimary,
													RelatedOpsBackup,
													CPDate,
													CPForm,
													DemurrageRate,
													TimeBar,
													AddressCommissionPercent,
													BrokerCommissionPercent,
													LaytimeAllowedLoad,
													LaytimeAllowedDisch,
													ShincReversible,
													VesselNameSnap,
													DemurrageAmountAgreed,
													CharterInvoiced,
													PaymentType,
													FreightLumpSumEntry,
													DischargeFAC,
													LaytimeOption,
													OwnersReference,
													CharterersReference,
													CurrencyInvoice,
													CharteringPicSnap,
													OperationsPicSnap,
													BrokerCommDemurrage,
													AddCommDeadFreight,
													DemurrageClaimReceived,
													VoyageNumber,
													LaycanToBeAmended,
													LaycanCancellingAmended,
													LaycanCommencementAmended,
													CurrencyCP,
													FixtureStatus,
													LaytimeAllowedTotalLoad,
													LaytimeAllowedTotalDisch,
													FrtRatePmt,
													BrokerFrtComm,
													P2FixtureRefNum,
													VesselFixedOfficial,
													LaycanCommencementOriginal,
													SPI_COA_Number,
													COA_Status,
													COA_Date,
													COA_AddendumDate,
													COA_AddendumExpiryDate,
													COA_AddendumCommencementDate,
													COA_RenewalDateDeclareBy,
													COA_ContractCommencement,
													COA_ContractCancelling,
													COA_Title_Admin,
													LaycanCancellingOriginal,
													LaycanCancellingFinal_QBC,
													LaycanCommencementFinal_QBC,
													SPIFixtureStatus,
													Region,
													LAF_Disch_Mtph_QBC,
													LAF_Load_Mtph_QBC,
													LAF_Total_hrs_QBC,
													LaytimeAllowedTypeFixture_QBC,
													FixtureType,
													GroupName,
													SPIOffice,
													Type1HashValue,
													RowCreatedDate,
													RowUpdatedDate,
													IsCurrentRow
												)

				values	(
							-1,				-- PostFixtureKey
							0,				-- PostFixtureAlternateKey
							'Unknown',		-- BrokerEmail
							'Unknown',		-- BrokerFirstName
							'Unknown',		-- BrokerLastName
							'Unknown',		-- BrokerFullName
							'Unknown',		-- OwnerFullStyle
							'Unknown',		-- ChartererFullStyle
							'Unknown',		-- OwnerParent
							'Unknown',		-- ChartererParent
							'Unknown',		-- RelatedOpsPrimary
							'Unknown',		-- RelatedOpsBackup
							'12/30/1899',	-- CPDate
							'Unknown',		-- CPForm
							0.0,			-- DemurrageRate
							0.0,			-- TimeBar
							0.0,			-- AddressCommissionPercent
							0.0,			-- BrokerCommissionPercent
							0.0,			-- LaytimeAllowedLoad
							0.0,			-- LaytimeAllowedDisch
							'Unknown',		-- ShincReversible
							'Unknown',		-- VesselNameSnap
							0.0,			-- DemurrageAmountAgreed
							'U',			-- CharterInvoiced
							'Unknown',		-- PaymentType
							0.0,			-- FreightLumpSumEntry
							'U',			-- DischargeFAC
							'Unknown',		-- LaytimeOption
							'Unknown',		-- OwnersReference
							'Unknown',		-- CharterersReference
							'Unknown',		-- CurrencyInvoice
							'Unknown',		-- CharteringPicSnap
							'Unknown',		-- OperationsPicSnap
							'U',			-- BrokerCommDemurrage
							'U',			-- AddCommDeadFreight
							'12/30/1899',	-- DemurrageClaimReceived
							'Unknown',		-- VoyageNumber
							'U',			-- LaycanToBeAmended
							'12/30/1899',	-- LaycanCancellingAmended
							'12/30/1899',	-- LaycanCommencementAmended
							'Unknown',		-- CurrencyCP
							'Unknown',		-- FixtureStatus
							0.0,			-- LaytimeAllowedTotalLoad
							0.0,			-- LaytimeAllowedTotalDisch
							0.0,			-- FrtRatePmt
							0.0,			-- BrokerFrtComm
							'Unknown',		-- P2FixtureRefNum
							'Unknown',		-- VesselFixedOfficial
							'12/30/1899',	-- LaycanCommencementOriginal
							0,				-- SPI_COA_Number
							'12/30/1899',	-- COA_Status
							'12/30/1899',	-- COA_Date
							'12/30/1899',	-- COA_AddendumDate
							'12/30/1899',	-- COA_AddendumExpiryDate
							'12/30/1899',	-- COA_AddendumCommencementDate
							'12/30/1899',	-- COA_RenewalDateDeclareBy
							'12/30/1899',	-- COA_ContractCommencement
							'12/30/1899',	-- COA_ContractCancelling
							'Unknown',		-- COA_Title_Admin
							'12/30/1899',	-- LaycanCancellingOriginal
							'12/30/1899',	-- LaycanCancellingFinal_QBC
							'12/30/1899',	-- LaycanCommencementFinal_QBC
							'Unknown',		-- SPIFixtureStatus
							'Unknown',		-- Region
							0.0,			-- LAF_Disch_Mtph_QBC
							0.0,			-- LAF_Load_Mtph_QBC
							0.0,			-- LAF_Total_hrs_QBC
							0.0,			-- LaytimeAllowedTypeFixture_QBC
							'Unknown',		-- FixtureType
							'Unknown',		-- GroupName
							'Unknown',		-- SPIOffice
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_PostFixture off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end