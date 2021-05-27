/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Staging.Dim_Port table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	07/19/2019	Added Area, Region
Brian Boswick	02/17/2020	Added LOARestriction, DraftRestriction, ProductRestriction
Brian Boswick	05/27/2021	Removed City, StateRegion
==========================================================================================================	
*/

drop table if exists Staging.Dim_Port;
go

create table Staging.Dim_Port
	(
		PortAlternateKey		int					not null,
		PortName				nvarchar(500)		not null,
		Country					nvarchar(100)		null,
		Comments				nvarchar(max)		null,
		Latitude				numeric(10, 4)		null,
		Longitude				numeric(10, 4)		null,
		PortCosts				nvarchar(250)		null,
		Area					varchar(150)		null,
		Region					varchar(150)		null,
		LOARestriction			numeric(10, 4)		null,
		DraftRestriction		numeric(10, 4)		null,
		ProductRestriction		varchar(500)		null,
		Type1HashValue			varbinary(16)		not null,
		RecordStatus			int					not null
		constraint [PK_Staging_Dim_Port_QBRecId] primary key clustered 
		(
			PortAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];