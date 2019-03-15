/*
==========================================================================================================
Author:			Brian Boswick
Create date:	03/14/2019
Description:	Creates the Staging.Dim_PortBerth table.  Stores distinct combinations of Port/Berth
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Staging.Dim_PortBerth;
go

create table Staging.Dim_PortBerth
	(
		PortAlternateKey			int					not null,
		BerthAlternateKey			int					not null,
		PortBerthName				nvarchar(500)		null,
		PortName					nvarchar(500)		null,
		BerthName					nvarchar(500)		null,
		City						nvarchar(250)		null,
		StateRegion					nvarchar(250)		null,
		Country						nvarchar(100)		null,
		Comments					nvarchar(max)		null,
		Latitude					numeric(10, 4)		null,
		Longitude					numeric(10, 4)		null,
		PortCosts					nvarchar(250)		null,
		Type1HashValue				varbinary(16)		not null,
		RecordStatus				int					not null
		constraint [PK_Staging_Dim_PortBerth_QBRecId] primary key clustered 
		(
			PortAlternateKey,
			BerthAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];