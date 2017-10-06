### GeoSpatial_query

**1. List of points in polygon and address points**

**2. Nearest Distance**

**3. Number of points in radius**

**4. Straight line distance**

**5. GeoSpatial Query optimization**

**6. Run optimization**



**List of points in polygon and address points:**

SELECT * FROM traffic T where ST_Contains(ci_wkb_geometry, (wkb_geometry))
UNION 
SELECT * FROM traffic T where ST_Intersects(ci_wkb_geometry, (wkb_geometry))


**Nearest Distance:**

SELECT MIN(ROUND(CAST(st_distance_sphere(st_makepoint(longitude, latitude),st_makepoint(LONGI, LAT)) As numeric),2)) AS Comp_Near 
FROM cc_CompetitorsJanuary2016 
WHERE wkb_geometry && pgis_makeboxgeom(box_longitude_upper,box_latitude_upper, box_longitude_lower,box_latitude_lower, 4326)::box3d  


**Number of points in radius: **

SELECT 
COUNT(DISTINCT(STREET))
FROM 
public.roadtype RT
WHERE 
wkb_geometry && pgis_makeboxgeom(box_longitude_upper,box_latitude_upper, box_longitude_lower,box_latitude_lower, 4326)::box3d AND 
ST_DWithin(Geography(wkb_geometry), Geography(ST_MakePoint( LONGI, LAT)), 100)

**Straight line distance:**

SELECT 			
MIN(ROUND(CAST(st_distance_sphere(st_makepoint(src.longitude, src.latitude),st_makepoint(LONGI, LAT)) AS NUMERIC),2)) 			
FROM 
focal_points_schools1 src
WHERE wkb_geometry && pgis_makeboxgeom(box_longitude_upper,box_latitude_upper, box_longitude_lower,box_latitude_lower, 4326)::box3d   

**Query Optimiztion**
wkb_geometry && pgis_makeboxgeom(box_longitude_upper,box_latitude_upper, box_longitude_lower,box_latitude_lower, 4326)::box3d AND 

--Presence of mine or quarry within 2.5 km radius (1 = present,0 = absent)

SELECT 
CASE WHEN 
((MIN(ROUND(CAST(st_distance_sphere(st_makepoint(longitude, latitude),st_makepoint(LONGI, LAT)) As numeric),2))) < 2500)
THEN 1 ELSE 0 END 
FROM focal_points_mine_quarry 
WHERE wkb_geometry && pgis_makeboxgeom(box_longitude_upper,box_latitude_upper, box_longitude_lower,box_latitude_lower, 4326)::box3d 


**Run optimization**
Arbitrary Function call to overcome Cartesian join

CREATE OR REPLACE FUNCTION public.runinsert_store_weather()
  RETURNS integer AS
$BODY$
DECLARE r RECORD;
DECLARE res  text;
BEGIN
    FOR r IN 
		
SELECT 
locationid, 
locationname, 
latitude, 
longitude, 
street1, 
street2,
round( CAST(longitude as numeric), 2)  - 0.07 AS box_longitude_UPPER,
round( CAST(latitude as numeric) , 2)  + 0.07 AS box_latitude_UPPER,
round( CAST(longitude as numeric), 2)  + 0.07 AS box_longitude_LOWER,
round( CAST(latitude as numeric) , 2)  - 0.07 AS box_latitude_LOWER
FROM public.geo_caltex_store 


-- Perform loop opertaion over the list of store in the	Caltex_Store table 
	LOOP
		PERFORM  insert_geo_caltex_store_nearest_caltex_store(
						r.locationid, 
						r.locationname, 
						r.latitude, 
						r.longitude, 
						r.street1, 
						r.street2, 						
						r.box_longitude_UPPER,  
						r.box_latitude_UPPER,
						r.box_longitude_LOWER,
						r.box_latitude_LOWER 
					  ) as output ;            		
    END LOOP;
return 1;
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 10000;

CREATE OR REPLACE FUNCTION public.insert_geo_caltex_store_weather(
    lid integer,
    locationname character varying,
    lat double precision,
    longi double precision,
    street1 character varying,
    street2 character varying,
    box_longitude_upper double precision,
    box_latitude_upper double precision,
    box_longitude_lower double precision,
    box_latitude_lower double precision)
  RETURNS void AS
$BODY$
BEGIN
	INSERT INTO public.geo_caltex_store_weather
		    (
		    locationid, locationname, latitude, longitude,   weather_station_distance, nearest_weather_station_id
		    )
	    VALUES 
		   (
	--locationid
			lid,
	--locationname
			locationname,
	--latitude
			lat,
	--latitude
			longi,
			(				
				SELECT 	MIN(round(CAST(st_distance_sphere(st_makepoint(longitude, latitude),st_makepoint(LONGI, LAT)) As numeric),2)  )
				FROM 	weather
				WHERE 		
					wkb_geometry && pgis_makeboxgeom(box_longitude_UPPER,  box_latitude_UPPER,box_longitude_LOWER,box_latitude_LOWER, 4326)::box3d 
					AND								
					(round(CAST(st_distance_sphere(st_makepoint(longitude, latitude),st_makepoint(LONGI, LAT)) As numeric),2)  ) > 1 								
			),		

			(
			SELECT 	station_id FROM 	weather
			WHERE 
			wkb_geometry && pgis_makeboxgeom(box_longitude_UPPER,  box_latitude_UPPER,box_longitude_LOWER,box_latitude_LOWER, 4326)::box3d 
			AND													
			(round(CAST(st_distance_sphere(st_makepoint(longitude, latitude),st_makepoint(LONGI, LAT)) As numeric),2)  ) > 1 
			order by round(CAST(st_distance_sphere(st_makepoint(longitude, latitude),st_makepoint(LONGI, LAT)) As numeric),2)  limit 1
			)	
	    );
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 10000;
