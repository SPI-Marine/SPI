set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadDim_TimeCharterer;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	08/20/2020
Description:	Creates the stored procedure LoadDim_TimeCharterer
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadDim_TimeCharterer
as
begin

	declare	@NewRecord		smallint = 1,
			@ExistingRecord smallint = 2,
			@Type1Change	smallint = 4,
			@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Dim_TimeCharterer', 'U') is not null
		truncate table Staging.Dim_TimeCharterer;

	begin try
		insert
				Staging.Dim_TimeCharterer with (tablock)
		select
				tc.RecordID,
				tc.[Status],
				tc.VaultTCFixtureNumber,
				tc.ContractType,
				tc.TCCPDate,
				tc.CharterParty,
				tc.VesselFixedAsOfficial,
				tc.OwnerRef,
				tc.ChartererRef,
				tc.PeriodUnits,
				tc.OptionAdditionalPeriod1,
				tc.OptionAdditionalPeriod1Units,
				tc.OptionAdditionalPeriod2,
				tc.OptionAdditionalPeriod2Units,
				tc.ContractCommencement,
				tc.ContractExpirey,
				tc.RenewalDateDeclareBy,
				tc.LaycanCommencement,
				tc.LaycanCancelling,
				tc.CurrencyForHire,
				tc.HirePaymentNotes,
				tc.HireRateFirstPeriod,
				tc.HireRateSecondPeriod,
				tc.HireRateThirdPeriod,
				tc.HirePayable,
				tc.FrequencyCommissionInvoiced,
				cast(replace(tc.AddressCommission, '%', '') as numeric(20, 6))			AddressCommission,
				cast(replace(tc.BrokerCommission, '%', '') as numeric(20, 6))			BrokerCommission,
				isnull(brokerpic.FirstName, '') + ' ' + isnull(brokerpic.LastName, '')	BrokerPIC,
				isnull(opspic.FirstName, '') + ' ' + isnull(opspic.LastName, '')		OpsPIC,
				office.OfficeName														SPIOffice,
				v.VesselName															Vessel,
				ownerfs.FullStyleName													OwnerFullStyle,
				chartererfs.FullStyleName												ChartererFullStyle,
				0 Type1HashValue,
				isnull(rs.RecordStatus, @NewRecord) RecordStatus
			from
				TimeCharterer tc (nolock)
					left join TeamMembers brokerpic (nolock)
						on tc.RelatedBrokerPICID = brokerpic.QBRecId
					left join TeamMembers opspic (nolock)
						on tc.RelatedOpsPICID = opspic.QBRecId					
					left join SpiOffices office (nolock)
						on office.QBRecId = tc.RelatedSPIOfficeID
					left join Vessels v (nolock)
						on v.QBRecId = tc.RelatedVesselID
					left join FullStyles ownerfs (nolock)
						on ownerfs.QBRecId = tc.RelatedOwnerFullStyleID
					left join FullStyles chartererfs (nolock)
						on chartererfs.QBRecId = tc.RelatedChartererFullStyleID
					left join	(
									select
											@ExistingRecord RecordStatus,
											TimeChartererAlternateKey
										from
											Warehouse.Dim_TimeCharterer (nolock)
								) rs
						on rs.TimeChartererAlternateKey = tc.RecordID;
	end try
	begin catch
		select @ErrorMsg = 'Staging TimeCharterer records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	

	-- Generate hash values for Type 1 changes. Only Type 1 SCDs
	begin try
		update
				Staging.Dim_TimeCharterer with (tablock)
			set
				-- Type 1 SCD
				Type1HashValue =	hashbytes	(
													'MD2',
													concat	(
																[Status],
																VaultTCFixtureNumber,
																ContractType,
																TCCPDate,
																CharterParty,
																VesselFixedAsOfficial,
																OwnerRef,
																ChartererRef,
																PeriodUnits,
																OptionAdditionalPeriod1,
																OptionAdditionalPeriod1Units,
																OptionAdditionalPeriod2,
																OptionAdditionalPeriod2Units,
																ContractCommencement,
																ContractExpirey,
																RenewalDateDeclareBy,
																LaycanCommencement,
																LaycanCancelling,
																CurrencyForHire,
																HirePaymentNotes,
																HireRateFirstPeriod,
																HireRateSecondPeriod,
																HireRateThirdPeriod,
																HirePayable,
																FrequencyCommissionInvoiced,
																AddressCommission,
																BrokerCommission,
																BrokerPIC,
																OpsPIC,
																SPIOffice,
																Vessel,
																OwnerFullStyle,
																ChartererFullStyle
															)
												);
		
		update
				Staging.Dim_TimeCharterer with (tablock)
			set
				RecordStatus += @Type1Change
			from
				Warehouse.Dim_TimeCharterer wc with (nolock)
			where
				wc.TimeChartererAlternateKey = Staging.Dim_TimeCharterer.TimeChartererAlternateKey
				and wc.Type1HashValue <> Staging.Dim_TimeCharterer.Type1HashValue;
	end try
	begin catch
		select @ErrorMsg = 'Updating hash values - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert new TimeCharterers into Warehouse table
	begin try
		insert
				Warehouse.Dim_TimeCharterer  with (tablock)
			select
					tc.TimeChartererAlternateKey,
					tc.[Status],
					tc.VaultTCFixtureNumber,
					tc.ContractType,
					tc.TCCPDate,
					tc.CharterParty,
					tc.VesselFixedAsOfficial,
					tc.OwnerRef,
					tc.ChartererRef,
					tc.PeriodUnits,
					tc.OptionAdditionalPeriod1,
					tc.OptionAdditionalPeriod1Units,
					tc.OptionAdditionalPeriod2,
					tc.OptionAdditionalPeriod2Units,
					tc.ContractCommencement,
					tc.ContractExpirey,
					tc.RenewalDateDeclareBy,
					tc.LaycanCommencement,
					tc.LaycanCancelling,
					tc.CurrencyForHire,
					tc.HirePaymentNotes,
					tc.HireRateFirstPeriod,
					tc.HireRateSecondPeriod,
					tc.HireRateThirdPeriod,
					tc.HirePayable,
					tc.FrequencyCommissionInvoiced,
					tc.AddressCommission,
					tc.BrokerCommission,
					tc.BrokerPIC,
					tc.OpsPIC,
					tc.SPIOffice,
					tc.Vessel,
					tc.OwnerFullStyle,
					tc.ChartererFullStyle,
					tc.Type1HashValue,
					getdate() RowStartDate,
					getdate() RowUpdatedDate,
					'Y' IsCurrentRow
				from
					Staging.Dim_TimeCharterer tc with (nolock)
				where
					tc.RecordStatus & @NewRecord = @NewRecord;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Update existing records that have changed
	begin try
		update
				Warehouse.Dim_TimeCharterer with (tablock)
			set
				[Status] = tc.[Status],
				VaultTCFixtureNumber = tc.VaultTCFixtureNumber,
				ContractType = tc.ContractType,
				TCCPDate = tc.TCCPDate,
				CharterParty = tc.CharterParty,
				VesselFixedAsOfficial = tc.VesselFixedAsOfficial,
				OwnerRef = tc.OwnerRef,
				ChartererRef = tc.ChartererRef,
				PeriodUnits = tc.PeriodUnits,
				OptionAdditionalPeriod1 = tc.OptionAdditionalPeriod1,
				OptionAdditionalPeriod1Units = tc.OptionAdditionalPeriod1Units,
				OptionAdditionalPeriod2 = tc.OptionAdditionalPeriod2,
				OptionAdditionalPeriod2Units = tc.OptionAdditionalPeriod2Units,
				ContractCommencement = tc.ContractCommencement,
				ContractExpirey = tc.ContractExpirey,
				RenewalDateDeclareBy = tc.RenewalDateDeclareBy,
				LaycanCommencement = tc.LaycanCommencement,
				LaycanCancelling = tc.LaycanCancelling,
				CurrencyForHire = tc.CurrencyForHire,
				HirePaymentNotes = tc.HirePaymentNotes,
				HireRateFirstPeriod = tc.HireRateFirstPeriod,
				HireRateSecondPeriod = tc.HireRateSecondPeriod,
				HireRateThirdPeriod = tc.HireRateThirdPeriod,
				HirePayable = tc.HirePayable,
				FrequencyCommissionInvoiced = tc.FrequencyCommissionInvoiced,
				AddressCommission = tc.AddressCommission,
				BrokerCommission = tc.BrokerCommission,
				BrokerPIC = tc.BrokerPIC,
				OpsPIC = tc.OpsPIC,
				SPIOffice = tc.SPIOffice,
				Vessel = tc.Vessel,
				OwnerFullStyle = tc.OwnerFullStyle,
				ChartererFullStyle = tc.ChartererFullStyle,
				Type1HashValue = tc.Type1HashValue,
				RowUpdatedDate = getdate()
			from
				Staging.Dim_TimeCharterer tc with (nolock)
			where
				tc.RecordStatus & @ExistingRecord = @ExistingRecord
				and tc.TimeChartererAlternateKey = Warehouse.Dim_TimeCharterer.TimeChartererAlternateKey
				and tc.RecordStatus & @Type1Change = @Type1Change;
	end try
	begin catch
		select @ErrorMsg = 'Updating existing records in Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Delete rows removed from source system
	begin try
		delete
				Warehouse.Dim_TimeCharterer with (tablock)
			where
				not exists	(
								select
										1
									from
										TimeCharterer tc with (nolock)
									where
										tc.RecordID = TimeChartererAlternateKey
							);
	end try
	begin catch
		select @ErrorMsg = 'Deleting removed records from Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch

	-- Insert Unknown record
	begin try
		if exists (select 1 from Warehouse.Dim_TimeCharterer where TimeChartererKey < 0)
		begin
			delete
					Warehouse.Dim_TimeCharterer with (tablock)
				where
					TimeChartererKey < 0;
		end
		else
		begin
			set identity_insert Warehouse.Dim_TimeCharterer on;
			insert
					Warehouse.Dim_TimeCharterer with (tablock)	(
																	TimeChartererKey,
																	TimeChartererAlternateKey,
																	[Status],
																	VaultTCFixtureNumber,
																	ContractType,
																	TCCPDate,
																	CharterParty,
																	VesselFixedAsOfficial,
																	OwnerRef,
																	ChartererRef,
																	PeriodUnits,
																	OptionAdditionalPeriod1,
																	OptionAdditionalPeriod1Units,
																	OptionAdditionalPeriod2,
																	OptionAdditionalPeriod2Units,
																	ContractCommencement,
																	ContractExpirey,
																	RenewalDateDeclareBy,
																	LaycanCommencement,
																	LaycanCancelling,
																	CurrencyForHire,
																	HirePaymentNotes,
																	HireRateFirstPeriod,
																	HireRateSecondPeriod,
																	HireRateThirdPeriod,
																	HirePayable,
																	FrequencyCommissionInvoiced,
																	AddressCommission,
																	BrokerCommission,
																	BrokerPIC,
																	OpsPIC,
																	SPIOffice,
																	Vessel,
																	OwnerFullStyle,
																	ChartererFullStyle,
																	Type1HashValue,
																	RowCreatedDate,
																	RowUpdatedDate,
																	IsCurrentRow
																)

				values	(
							-1,				-- TimeChartererKey
							-1,				-- TimeChartererAlternateKey
							'Unknown',		-- [Status]
							'Unknown',		-- VaultTCFixtureNumber
							'Unknown',		-- ContractType
							'12/30/1899',	-- TCCPDate
							'Unknown',		-- CharterParty
							'Unknown',		-- VesselFixedAsOfficial
							'Unknown',		-- OwnerRef
							'Unknown',		-- ChartererRef
							'Unknown',		-- PeriodUnits
							0.0,			-- OptionAdditionalPeriod1
							'Unknown',		-- OptionAdditionalPeriod1Units
							0.0,			-- OptionAdditionalPeriod2
							'Unknown',		-- OptionAdditionalPeriod2Units
							'12/30/1899',	-- ContractCommencement
							'12/30/1899',	-- ContractExpirey
							'12/30/1899',	-- RenewalDateDeclareBy
							'12/30/1899',	-- LaycanCommencement
							'12/30/1899',	-- LaycanCancelling
							'Unknown',		-- CurrencyForHire
							'Unknown',		-- HirePaymentNotes
							0.0,			-- HireRateFirstPeriod
							0.0,			-- HireRateSecondPeriod
							0.0,			-- HireRateThirdPeriod
							'Unknown',		-- HirePayable
							'Unknown',		-- FrequencyCommissionInvoiced
							0.0,			-- AddressCommission
							0.0,			-- BrokerCommission
							'Unknown',		-- BrokerPIC
							'Unknown',		-- OpsPIC
							'Unknown',		-- SPIOffice
							'Unknown',		-- Vessel
							'Unknown',		-- OwnerFullStyle
							'Unknown',		-- ChartererFullStyle
							0,				-- Type1HashValue
							getdate(),		-- RowCreatedDate
							getdate(),		-- RowUpdatedDate
							'Y'				-- IsCurrentRow
						);
			set identity_insert Warehouse.Dim_TimeCharterer off;
		end
	end try
	begin catch
		select @ErrorMsg = 'Inserting Unknown record into Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end