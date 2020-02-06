set ansi_nulls on;
go
set quoted_identifier on;
go

drop procedure if exists ETL.LoadFact_MarketInfo;
go

/*
==========================================================================================================
Author:			Brian Boswick
Create date:	02/04/2020
Description:	Creates the LoadFact_MarketInfo stored procedure
Changes
Developer		Date		Change
----------------------------------------------------------------------------------------------------------
==========================================================================================================	
*/

create procedure ETL.LoadFact_MarketInfo
as
begin
	set nocount on;

	declare	@ErrorMsg		varchar(1000);

	-- Clear Staging table
	if object_id(N'Staging.Fact_MarketInfo', 'U') is not null
		truncate table Staging.Fact_MarketInfo;

	begin try
		insert
				Staging.Fact_MarketInfo with (tablock)	(
															MarketInfoAlternateKey,
															ProductKey,
															LoadPortKey,
															DischargePortKey,
															ReportDateKey,
															CommencementDateKey,
															CancellingDateKey,
															VesselKey,
															OwnerKey,
															ChartererKey,
															LoadPort2,
															DischargePort2,
															DischargePort3,
															MarketInfoType,
															Unit,
															FreightRatePayment,
															ProductQuantity
														)
		select
				mi.RecordID						MarketInfoAlternateKey,
				isnull(wp.ProductKey, -1)		ProductKey,
				isnull(loadport1.PortKey, -1)	LoadPortKey,
				isnull(dischport1.PortKey, -1)	DischargePortKey,
				isnull(rd.DateKey, -1)			ReportDateKey,
				isnull(commdate.DateKey, -1)	CommencementDateKey,
				isnull(canceldate.DateKey, -1)	CancellingDateKey,
				isnull(v.VesselKey, -1)			VesselKey,
				isnull(o.OwnerKey, -1)			OwnerKey,
				isnull(ch.ChartererKey, -1)		ChartererKey,
				loadport2.PortName				LoadPort2,
				dischport2.PortName				DischargePort2,
				dischport3.PortName				DischargePort3,
				mi.[Type]						MarketInfoType,
				mi.Unit							Unit,
				mi.FreightRatePMTEntry			FreightRatePayment,
				mi.ProductQuantity_ENTRY		ProductQuantity
			from
				MarketInfo mi
					left join Warehouse.Dim_Product wp
						on wp.ProductAlternateKey = mi.RelatedProductID
					left join Warehouse.Dim_Port loadport1
						on loadport1.PortAlternateKey = mi.RelatedDischPort1ID
					left join Warehouse.Dim_Port dischport1
						on dischport1.PortAlternateKey = mi.RelatedDischPort1ID
					left join Warehouse.Dim_Port loadport2
						on loadport2.PortAlternateKey = mi.RelatedDischPort2ID
					left join Warehouse.Dim_Port dischport2
						on dischport2.PortAlternateKey = mi.RelatedDischPort2ID
					left join Warehouse.Dim_Port dischport3
						on dischport3.PortAlternateKey = mi.DischPort3RelatedID
					left join Warehouse.Dim_Calendar rd
						on rd.FullDate = convert(date, mi.ReportDate)
					left join Warehouse.Dim_Calendar commdate
						on commdate.FullDate = convert(date, mi.Commencement)
					left join Warehouse.Dim_Calendar canceldate
						on canceldate.FullDate = convert(date, mi.Cancelling)
					left join Warehouse.Dim_Vessel v
						on v.VesselAlternateKey = mi.RelatedVesselID
					left join Warehouse.Dim_Owner o
						on o.OwnerAlternateKey = mi.RelatedOwnerLinkID
					left join Warehouse.Dim_Charterer ch
						on ch.ChartererAlternateKey = mi.RelatedChartererID;
	end try
	begin catch
		select @ErrorMsg = 'Staging MarketInfo records - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch	
	
	-- Clear Warehouse table
	if object_id(N'Warehouse.Fact_MarketInfo', 'U') is not null
		truncate table Warehouse.Fact_MarketInfo;

	-- Insert records into Warehouse table
	begin try
		insert
				Warehouse.Fact_MarketInfo with (tablock)	(
																MarketInfoAlternateKey,
																ProductKey,
																LoadPortKey,
																DischargePortKey,
																ReportDateKey,
																CommencementDateKey,
																CancellingDateKey,
																VesselKey,
																OwnerKey,
																ChartererKey,
																LoadPort2,
																DischargePort2,
																DischargePort3,
																MarketInfoType,
																Unit,
																FreightRatePayment,
																ProductQuantity,
																RowCreatedDate
															)
			select
					fmi.MarketInfoAlternateKey,
					fmi.ProductKey,
					fmi.LoadPortKey,
					fmi.DischargePortKey,
					fmi.ReportDateKey,
					fmi.CommencementDateKey,
					fmi.CancellingDateKey,
					fmi.VesselKey,
					fmi.OwnerKey,
					fmi.ChartererKey,
					fmi.LoadPort2,
					fmi.DischargePort2,
					fmi.DischargePort3,
					fmi.MarketInfoType,
					fmi.Unit,
					fmi.FreightRatePayment,
					fmi.ProductQuantity,
					getdate()
				from
					Staging.Fact_MarketInfo fmi;
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end