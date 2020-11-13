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
Brian Boswick	02/20/2020	Added BasisDataEntry
Brian Boswick	10/16/2020	Added PandC
Brian Boswick	11/01/2020	Added LastModifiedBy
Brian Boswick	11/05/2020	Modified ETL for new quantity ranges
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
															OwnerParentKey,
															ChartererParentKey,
															ProductQuantityKey,
															LoadPort2,
															DischargePort2,
															DischargePort3,
															MarketInfoType,
															Unit,
															BasisDataEntry,
															PandC,
															LastModifiedBy,
															Comments,
															FreightRatePayment,
															ProductQuantity
														)
		select
				mi.RecordID							MarketInfoAlternateKey,
				isnull(wp.ProductKey, -1)			ProductKey,
				isnull(loadport1.PortKey, -1)		LoadPortKey,
				isnull(dischport1.PortKey, -1)		DischargePortKey,
				isnull(rd.DateKey, -1)				ReportDateKey,
				isnull(commdate.DateKey, -1)		CommencementDateKey,
				isnull(canceldate.DateKey, -1)		CancellingDateKey,
				isnull(v.VesselKey, -1)				VesselKey,
				isnull(o.OwnerParentKey, -1)		OwnerParentKey,
				isnull(ch.ChartererParentKey, -1)	ChartererParentKey,
				isnull(pq.ProductQuantityKey, -1)	ProductQuantityKey,
				loadport2.PortName					LoadPort2,
				dischport2.PortName					DischargePort2,
				dischport3.PortName					DischargePort3,
				mi.[Type]							MarketInfoType,
				mi.Unit								Unit,
				mi.BasisDataEntry					BasisDataEntry,
				mi.PandC							PandC,
				mi.LastModifiedBy					LastModifiedBy,
				mi.Notes							Comments,
				mi.FreightRatePMTEntry				FreightRatePayment,
				mi.ProductQuantity_ENTRY			ProductQuantity
			from
				MarketInfo mi
					left join Warehouse.Dim_Product wp with (nolock)
						on wp.ProductAlternateKey = mi.RelatedProductID
					left join Warehouse.Dim_Port loadport1 with (nolock)
						on loadport1.PortAlternateKey = mi.RelatedLoadPort1ID
					left join Warehouse.Dim_Port dischport1 with (nolock)
						on dischport1.PortAlternateKey = mi.RelatedDischPort1ID
					left join Warehouse.Dim_Port loadport2 with (nolock)
						on loadport2.PortAlternateKey = mi.RelatedLoadPort2ID
					left join Warehouse.Dim_Port dischport2 with (nolock)
						on dischport2.PortAlternateKey = mi.RelatedDischPort2ID
					left join Warehouse.Dim_Port dischport3 with (nolock)
						on dischport3.PortAlternateKey = mi.DischPort3RelatedID
					left join Warehouse.Dim_Calendar rd with (nolock)
						on rd.FullDate = convert(date, mi.ReportDate)
					left join Warehouse.Dim_Calendar commdate with (nolock)
						on commdate.FullDate = convert(date, mi.Commencement)
					left join Warehouse.Dim_Calendar canceldate with (nolock)
						on canceldate.FullDate = convert(date, mi.Cancelling)
					left join Warehouse.Dim_Vessel v with (nolock)
						on v.VesselAlternateKey = mi.RelatedVesselID
					left join Warehouse.Dim_OwnerParent o with (nolock)
						on o.OwnerParentAlternateKey = mi.RelatedOwnerLinkID
					left join Warehouse.Dim_ChartererParent ch with (nolock)
						on ch.ChartererParentAlternateKey = mi.RelatedChartererID
					left join Warehouse.Dim_ProductQuantity pq with (nolock)
						on convert(decimal(18, 4), mi.ProductQuantity_ENTRY) >= pq.MinimumQuantity
							and convert(decimal(18, 4), mi.ProductQuantity_ENTRY) < pq.MaximumQuantity;
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
																OwnerParentKey,
																ChartererParentKey,
																ProductQuantityKey,
																LoadPort2,
																DischargePort2,
																DischargePort3,
																MarketInfoType,
																Unit,
																BasisDataEntry,
																PandC,
																LastModifiedBy,
																Comments,
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
					fmi.OwnerParentKey,
					fmi.ChartererParentKey,
					fmi.ProductQuantityKey,
					fmi.LoadPort2,
					fmi.DischargePort2,
					fmi.DischargePort3,
					fmi.MarketInfoType,
					fmi.Unit,
					fmi.BasisDataEntry,
					fmi.PandC,
					fmi.LastModifiedBy,
					fmi.Comments,
					fmi.FreightRatePayment,
					fmi.ProductQuantity,
					getdate()
				from
					Staging.Fact_MarketInfo fmi with (nolock);
	end try
	begin catch
		select @ErrorMsg = 'Loading Warehouse - ' + error_message();
		throw 51000, @ErrorMsg, 1;
	end catch
end