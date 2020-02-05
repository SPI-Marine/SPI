/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the Staging.Dim_Charterer table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_Charterer;
go

create table Staging.Dim_Charterer
	(
		ChartererAlternateKey		int					not null,
		ChartererName				varchar(500)		not null,
		ChartererType				varchar(250)		null,
		Type1HashValue				varbinary(16)		not null,
		RecordStatus				int					not null
		constraint [PK_Staging_Dim_Charterer_QBRecId] primary key clustered 
		(
			ChartererAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];