WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-04-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-05-01T00:00:00Z`))
    ),

  "$bucket:0%0" AS
    (SELECT "$source"."FlightDelayType" AS "$key:0%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     GROUP BY "$source"."FlightDelayType"
     ORDER BY "$doc_count" DESC
     LIMIT 10
    ),

  "$bucket:0:1%0" AS
    (SELECT "$source"."FlightDelayType" AS "$key:0%0",
            DATE_TRUNC(DAY,"$source"."timestamp") AS "$key:0:1%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     WHERE ("$source"."FlightDelayType" IN (SELECT "$selection"."$key:0%0"
     FROM "$bucket:0%0" AS "$selection"))
     GROUP BY "$source"."FlightDelayType",
              DATE_TRUNC(DAY,"$source"."timestamp")
     ORDER BY "$key:0:1%0" ASC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:0%0"
  ) AS "$bucket:0%0",

  (SELECT *
   FROM "$bucket:0:1%0"
  ) AS "$bucket:0:1%0"