/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/09/2020
Description:	Creates the Warehouse.RowLevelSecurity table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop table if exists Warehouse.RowLevelSecurity;
go

create table Warehouse.RowLevelSecurity
	(
		PortalUserId						varchar(500)		null,
		PortalUserEmail						varchar(500)		null,
		PermissionLevelId					smallint			null,
		PermissionLevelName					varchar(250)		null,
		ChartererParentRlsKey				varchar(100)		null,
		ChartererRlsKey						varchar(100)		null,
		ChartererParentRlsKey				varchar(100)		null,
		OwnerParentRlsKey					varchar(100)		null,
		OwnerRlsKey							varchar(100)		null,
		ProductRlsKey						varchar(100)		null,
		LoadRegionRlsKey					varchar(100)		null,
		DischargeRegionRlsKey				varchar(100)		null
	) on [primary];