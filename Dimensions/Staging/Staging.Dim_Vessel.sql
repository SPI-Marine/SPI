/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Staging.Dim_Vessel table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_Vessel;
go

create table Staging.Dim_Vessel
	(
		VesselAlternateKey		int					not null,
		VesselName				nvarchar(500)		not null,
		Draft					decimal(18, 2)		null,
		YearBuilt				int					null,
		Coils					nvarchar(max)		null,
		DeadWeight				decimal(18, 2)		null,
		Beam					decimal(18, 2)		null,
		LOA						decimal(18, 2)		null,
		Yard					nvarchar(max)		null,
		IceEntry				nvarchar(max)		null,
		RegisteredOwner			nvarchar(max)		null,
		CleanDirty				nvarchar(max)		null,
		TcCandidate				bit					null,
		IMOType					nvarchar(max)		null,
		Tanks					decimal(18, 2)		null,
		Pumps					decimal(18, 2)		null,
		Segs					decimal(18, 2)		null,
		CBM						decimal(18, 2)		null,
		Hull					nvarchar(max)		null,
		ExName					nvarchar(max)		null,
		Comments				nvarchar(max)		null,
		Trade					nvarchar(max)		null,
		CountryOfBuild			nvarchar(max)		null,
		Flag					nvarchar(max)		null,
		DataSource				nvarchar(max)		null,
		CommercialOwnerOperator nvarchar(max)		null,
		ArchivedVsl				bit					null,
		ReasonForArchive		nvarchar(max)		null,
		IGSType					nvarchar(max)		null,
		[Status]				nvarchar(max)		null,
		IMOCertificateRemoved	nvarchar(max)		null,
		KTRNumber				nvarchar(max)		null,
		STSTCBM					decimal(18, 2)		null,
		MarineLineCBM			decimal(18, 2)		null,
		InterlineCBM			decimal(18, 2)		null,
		EpoxyCBM				decimal(18, 2)		null,
		ZincCBM					decimal(18, 2)		null,
		IMO1CBM					decimal(18, 2)		null,
		IMO2CBM					decimal(18, 2)		null,
		IMO3CBM					decimal(18, 2)		null,
		YdNo					nvarchar(max)		null,
		BuildDate				date				null,
		NBContractDate			date				null,
		RetiredDate				date				null,
		KTRChangeDate			date				null,
		DeliveryDate			date				null,
		VesselType				nvarchar(max)		null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null
		constraint [PK_Staging_Dim_Vessel_QBRecId] primary key clustered 
		(
			VesselAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];