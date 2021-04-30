drop table if exists Staging.Dim_Currency;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	04/30/2021
Description:	Creates the Staging.Dim_Currency table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create table Staging.Dim_Currency
	(
		CurrencyCode			varchar(5)			not null,
		CurrencyName			varchar(100)		not null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null,
		constraint [PK_Staging_Dim_Currency_CurrencyCode] primary key clustered 
		(
			CurrencyCode asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];