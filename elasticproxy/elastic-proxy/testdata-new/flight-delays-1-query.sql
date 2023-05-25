WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:2%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",864000) AS "$key:2%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     GROUP BY TIME_BUCKET("$source"."timestamp",864000)
     ORDER BY "$key:2%0" ASC
    ),

  "$bucket:2:3%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",864000) AS "$key:2%0",
            "$source"."FlightDelayType" AS "$key:2:3%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     WHERE (TIME_BUCKET("$source"."timestamp",864000) IN (SELECT "$selection"."$key:2%0"
     FROM "$bucket:2%0" AS "$selection"))
     GROUP BY TIME_BUCKET("$source"."timestamp",864000),
              "$source"."FlightDelayType"
     HAVING (ROW_NUMBER() OVER (PARTITION BY TIME_BUCKET("$source"."timestamp",864000) ORDER BY COUNT(*) DESC) <= 10)
     ORDER BY "$doc_count" DESC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:2%0"
  ) AS "$bucket:2%0",

  (SELECT *
   FROM "$bucket:2:3%0"
  ) AS "$bucket:2:3%0"