WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (("$source"."FlightDelay" = TRUE) AND (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`)))
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count"