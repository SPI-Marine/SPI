set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_Position;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	01/27/2020
Description:	Creates the LoadFact_Position stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_Position
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000),
			@NewRecord			int = 1,
			@ExistingRecord		int = 2;

	-- Clear Staging table
	if object_id(N'Staging.Fact_Position', 'U') is not null
		truncate table Staging.Fact_Position;

	begin try
		insert
				Staging.Fact_Position with (tablock)	(	
															PositionAlternateKey,
															ProductKey,
															PortKey,
															VesselKey,
															OpenDateKey,
															EndDateKey,
															Comments,
															StatusCalculation,
															LastCargo,
															FOFSA,
															PositionType
														)	
		select
				p.RecordID							PositionAlternateKey,
				isnull(prod.ProductKey, -1)			ProductKey,
				isnull(wport.PortKey, -1)			PortKey,
				isnull(v.VesselKey, -1)				VesselKey,
				isnull(od.DateKey, -1)				OpenDateKey,
				isnull(ed.DateKey, -1)				EndDateKey,
				p.Comments							Comments,
				p.StatusCalculation_ADMIN			StatusCalculation,
				p.LastCargo							LastCargo,
				p.FOFSA_prod						FOFSA,
				p.PositionType						PositionType
			from
				Positions p
					left join Warehouse.Dim_Calendar od
						on convert(date, p.DateOpen) = od.FullDate
					left join Warehouse.Dim_Calendar ed
						on convert(date, p.EndDate) = ed.FullDate
					left join Warehouse.Dim_Vessel v
						on v.VesselAlternateKey = p.RelatedVesselID
					left join Warehouse.Dim_Port wport
						on wport.PortAlternateKey = p.RelatedPortID
					left join Warehouse.Dim_Product prod
						on prod.ProductAlternateKey = p.RelatedProductID;
	end try
	begin catch
		select @ErrorMsg = 'Staging records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_Position', 'U') is not null
		truncate table Warehouse.Fact_Position;

	-- Insert records into Warehouse table
	begin try
		insert
				Warehouse.Fact_Position with (tablock)	(
															PositionAlternateKey,
															ProductKey,
															PortKey,
															VesselKey,
															OpenDateKey,
															EndDateKey,
															Comments,
															StatusCalculation,
															LastCargo,
															FOFSA,
															PositionType,
															RowCreatedDate
														)
			select
					sp.PositionAlternateKey,
					sp.ProductKey,
					sp.PortKey,
					sp.VesselKey,
					sp.OpenDateKey,
					sp.EndDateKey,
					sp.Comments,
					sp.StatusCalculation,
					sp.LastCargo,
					sp.FOFSA,
					sp.PositionType,
					getdate()
				from
					Staging.Fact_Position sp;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end