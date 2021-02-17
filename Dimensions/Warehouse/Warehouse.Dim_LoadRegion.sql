/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/18/2020
Description:	Creates the Warehouse.Dim_LoadRegion view used as a role-playing dimension
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop view if exists Warehouse.Dim_LoadRegion;
go

create view Warehouse.Dim_LoadRegion
as
	select
			RegionKey												LoadRegionKey,
			RegionAlternateKey										LoadRegionAlternateKey,
			'lr_' + convert(varchar(50), RegionAlternateKey)		LoadRegionRlsKey,
			RegionName												LoadRegionName,
			IsCurrentRow
		from
			Warehouse.Dim_Region;