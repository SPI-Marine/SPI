/*
==========================================================================================================
Author:			Brian Boswick
Create date:	12/18/2020
Description:	Creates the Warehouse.Dim_DischargeRegion view used as a role-playing dimension
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

drop view if exists Warehouse.Dim_DischargeRegion;
go

create view Warehouse.Dim_DischargeRegion
as
	select
			RegionKey												DischargeRegionKey,
			RegionAlternateKey										DischargeRegionAlternateKey,
			'dr_' + convert(varchar(50), RegionAlternateKey)		DischargeRegionRlsKey,
			RegionName												DischargeRegionName,
			IsCurrentRow
		from
			Warehouse.Dim_Region;