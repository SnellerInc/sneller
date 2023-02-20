WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:2%0" AS
    (SELECT "$source"."DestWeather" AS "$key:2%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     GROUP BY "$source"."DestWeather"
     ORDER BY "$doc_count" DESC
     LIMIT 10
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:2%0"
  ) AS "$bucket:2%0"