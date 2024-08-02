drop table if exists dp_bi.shipments;
create table if not exists dp_bi.shipments as
--with base_shipments as (
select 
    -- dates
    date(created_at) as created_date
    , datetime(created_at) as created_at
    , date(arrival_date) as arrival_date
    , date(original_arrival_date) as original_arrival_date
    , date(ship_date) as ship_date
    , date(original_ship_date) as original_ship_date
    , date(delivered_at) as delivered_date
    , datetime(delivered_at) as delivered_at
    , date(in_transit_at) as in_transit_date
    , datetime(in_transit_at) as in_transit_at
    , datetime(forced_date) as forced_action_at
    , date(forced_date) as forced_action_date
    , datetime(forced_delivered_at) as force_delivered_at
    , date(force_delivered_at) as forced_delivered_date
    , date(estimated_carrier_delivery_date) as estimated_carrier_delivery_date
    -- dimensions - ids
    , _id as shipment_id
    , order_id
    , tracking_id
    , replace(tracking_id, ' -r', '') as base_tracking_id
    , carrier_id
    , carrier_service_level_id
    , carrier_tracking_id
    , state as shipment_state
    , leg_id
    , nullif(club_session_id, '<NA>') as club_session_id
    , nullif(micro_site_id, '<NA>') as micro_site_id
    , traveler_id
    , user_id
    -- dimensions
    , padding_days
    , range_padding_days
    , deliverable_padding_days
    , transit_days
    , nullif(forced_action, '<NA>') as forced_action
    , reactivated
    , destination_ship_point.cruise as destination_cruise_flag
    , destination_ship_point.city as destination_city
    , destination_ship_point.company_name as destination_name
    , destination_ship_point.delivery_address_line as destination_address_line
    , destination_ship_point.delivery_address_line_1 as destination_address_line_1
    , concat(destination_ship_point.delivery_address_line,' ',destination_ship_point.delivery_address_line_1) as full_destination_address_line
    , nullif(destination_ship_point.facility_id, '<N/A>') as destination_facility_id
    , destination_ship_point.state as destination_state
    , destination_ship_point.zip5 as destination_zip_code
	, destination_ship_point.country_code as destination_country_code
    , origination_ship_point.cruise as origination_cruise_flag
    , origination_ship_point.city as origination_city
    , origination_ship_point.company_name as origination_name
    , origination_ship_point.delivery_address_line as origination_address_line
    , origination_ship_point.delivery_address_line_1 as origination_address_line_1
    , concat(origination_ship_point.delivery_address_line,' ',origination_ship_point.delivery_address_line_1) as full_origination_address_line
    , nullif(origination_ship_point.facility_id, '<N/A>') as origination_facility_id
    , origination_ship_point.state as origination_state
    , origination_ship_point.zip5 as origination_zip_code
	, origination_ship_point.country_code as origination_country_code
    , case when destination_ship_point.cruise = 1 or origination_ship_point.cruise = 1 then 1 else 0 end as cruise_shipment_flag
from mongo_land.v5_shipments
--),

