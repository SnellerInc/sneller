WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$source"
   ORDER BY "$source"."timestamp" DESC
   LIMIT 12
  ) AS "$hits"