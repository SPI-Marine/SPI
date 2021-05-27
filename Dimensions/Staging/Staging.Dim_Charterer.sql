drop table if exists Staging.Dim_Charterer;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the Staging.Dim_Charterer table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	08/13/2020	Source data from FullStyles table
Brian Boswick	12/16/2020	Added ChartererParentAlternateKey/ChartererParentRlsKey/ChartererRlsKey for RLS
Brian Boswick	05/27/2021	Removed GroupName
==========================================================================================================	
*/

create table Staging.Dim_Charterer
	(
		ChartererAlternateKey		int					not null,
		ChartererParentAlternateKey	int					null,
		ChartererRlsKey				varchar(100)		null,
		ChartererParentRlsKey		varchar(100)		null,
		FullStyleName				varchar(500)		null,
		ChartererParentName			varchar(500)		null,
		[Type]						varchar(500)		null,
		[Address]					varchar(500)		null,
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