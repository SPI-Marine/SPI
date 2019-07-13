/*
==========================================================================================================
Author:			Brian Boswick
Create date:	07/12/2019
Description:	Creates the Staging.Dim_Country table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_Country;
go

create table Staging.Dim_Country
	(
		CountryAlternateKey		int					not null,
		CountryName				nvarchar(250)		not null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null,
		constraint [PK_Staging_Dim_Country_QBRecId] primary key clustered 
		(
			CountryAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];