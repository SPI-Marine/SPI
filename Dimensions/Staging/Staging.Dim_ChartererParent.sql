/*
==========================================================================================================
Author:			Brian Boswick
Create date:	09/15/2020
Description:	Creates the Staging.Dim_ChartererParent table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_ChartererParent;
go

create table Staging.Dim_ChartererParent
	(
		ChartererParentAlternateKey		int					not null,
		ChartererParentName				varchar(500)		null,
		Notes							varchar(5000)		null,
		[Type]							varchar(500)		null,
		Type1HashValue					varbinary(16)		not null,
		RecordStatus					int					not null
		constraint [PK_Staging_Dim_ChartererParent_QBRecId] primary key clustered 
		(
			ChartererParentAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];