/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/28/2020
Description:	Creates the Staging.Dim_COA table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_COA;
go

create table Staging.Dim_COA
	(
		COAAlternateKey				int					not null,
		COATitle					varchar(500)		null,
		AddressCommission			decimal(20, 6)		null,
		BrokerCommission			decimal(20, 6)		null,
		[Status]					varchar(50)			null,
		[P&C]						varchar(50)			null,
		COADate						date				null,
		AddendumDate				date				null,
		AddendumExpiryDate			date				null,
		AddendumCommencementDate	date				null,
		RenewalDeclareByDate		date				null,
		ContractCommencementDate	date				null,
		ContractCancellingDate		date				null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null
		constraint [PK_Staging_Dim_COA_QBRecId] primary key clustered 
		(
			COAAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];