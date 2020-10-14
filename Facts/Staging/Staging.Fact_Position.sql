drop table if exists Staging.Fact_Position;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/27/2020
Description:	Creates the Staging.Fact_Position table
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
Brian Boswick	02/10/2020	Added OwnerKey ETL logic
Brian Boswick	02/12/2020	Added Direction and ShippingArea
Brian Boswick	09/21/2020	Changed OwnerKey to OwnerParentKey
==========================================================================================================	
*/

create table Staging.Fact_Position
	(
		PositionAlternateKey					int					not null,
		ProductKey								int					not null,
		PortKey									int					not null,
		DischargePortKey						int					not null,
		VesselKey								int					not null,
		OpenDateKey								int					not null,
		EndDateKey								int					not null,
		OwnerParentKey							int					not null,
		Comments								varchar(5000)		null,		-- Degenerate Dimension Attributes
		StatusCalculation						varchar(500)		null,
		LastCargo								varchar(500)		null,
		FOFSA									varchar(500)		null,
		PositionType							varchar(500)		null,
		Direction								varchar(500)		null,
		ShippingArea							varchar(500)		null,
		constraint [PK_Staging_Fact_Position_AltKey] primary key clustered 
		(
			PositionAlternateKey asc
		)
			with 
				(pad_index = off, statistics_norecompute = off, ignore_dup_key = off, allow_row_locks = on, allow_page_locks = on) 
			on [primary]
	) on [primary];