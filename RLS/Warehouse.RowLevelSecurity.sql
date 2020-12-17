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
		RecordID							int					null,
		Product								varchar(500)		null,
		ProductAlternateKey					int					null,
		Charterer							varchar(500)		null,
		ChartererAlternateKey				int					null,
		ChartererParent						varchar(500)		null,
		ChartererParentAlternateKey			int					null,
		[Owner]								varchar(500)		null,
		OwnerAlternateKey					int					null,
		OwnerParent							varchar(500)		null,
		OwnerParentAlternateKey				int					null,
		UserName							varchar(500)		null,
		LoadRegion							varchar(500)		null,
		LoadRegionAlternateKey				int					null,
		DischargeRegion						varchar(500)		null,
		DischargeRegionAlternateKey			int					null,
		FullStyleName						varchar(500)		null,
		[GUID]								varchar(500)		null,
		MinCPDateToPull						varchar(500)		null,
	) on [primary];