WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
    ),

  "$bucket:resource_id%0" AS
    (SELECT "$source"."OriginCountry" AS "$key:resource_id%0",
            COUNT(*) AS "$doc_count",
            COUNT(DISTINCT "$source"."DestAirportID") AS "dest_airport_count",
            COUNT(DISTINCT "$source"."OriginAirportID") AS "orig_airport_count"
     FROM "$source"
     GROUP BY "$source"."OriginCountry"
     ORDER BY "$doc_count" DESC
     LIMIT 1000
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:resource_id%0"
  ) AS "$bucket:resource_id%0"