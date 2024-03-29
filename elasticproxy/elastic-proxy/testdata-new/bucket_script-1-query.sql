WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:2%0" AS
    (SELECT ((30 * WIDTH_BUCKET(("$source"."FlightDelayMin" + 15),0,30000,1000)) - 30) AS "$key:2%0",
            COUNT(*) AS "$doc_count",
            COUNT(DISTINCT "$source"."DestCountry") AS "destCountries",
            COUNT(DISTINCT "$source"."OriginCountry") AS "origCountries"
     FROM "$source"
     GROUP BY ((30 * WIDTH_BUCKET(("$source"."FlightDelayMin" + 15),0,30000,1000)) - 30)
     ORDER BY "$key:2%0" ASC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:2%0"
  ) AS "$bucket:2%0"