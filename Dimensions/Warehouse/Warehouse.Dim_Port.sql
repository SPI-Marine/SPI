/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/30/2018
Description:	Creates the Warehouse.Dim_Port table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.Dim_Port;
go

create table Warehouse.Dim_Port
	(
		PortKey					int					not null identity(1, 1),
		PortAlternateKey		int					not null,
		PortName				nvarchar(500)		not null,
		City					nvarchar(250)		null,
		StateRegion				nvarchar(250)		null,
		Country					nvarchar(100)		null,
		Comments				nvarchar(max)		null,
		Latitude				nvarchar(100)		null,
		Longitude				nvarchar(100)		null,
		PortCosts				nvarchar(250)		null,
		Type1HashValue			varbinary(16)		not null,
		RowCreatedDate			date				not null,
		RowUpdatedDate			date				not null,
		IsCurrentRow			char(1)				not null,
		constraint [PK_Warehouse_Dim_Port_Key] primary key clustered 
		(
			PortAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];