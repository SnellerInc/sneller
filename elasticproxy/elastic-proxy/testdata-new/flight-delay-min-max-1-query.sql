WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:%0" AS
    (SELECT MAX("$source"."FlightDelayMin") AS "maxAgg",
            MIN("$source"."FlightDelayMin") AS "minAgg"
     FROM "$source"
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:%0"
  ) AS "$bucket:%0"