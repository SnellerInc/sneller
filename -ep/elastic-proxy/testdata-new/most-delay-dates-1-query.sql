WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:cancellation_dates%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",86400) AS "$key:cancellation_dates%0",
            COUNT(*) AS "$doc_count",
            SUM("$source"."FlightDelayMin") AS "total_delay_min"
     FROM "$source"
     GROUP BY TIME_BUCKET("$source"."timestamp",86400)
     ORDER BY "$key:cancellation_dates%0" ASC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:cancellation_dates%0"
  ) AS "$bucket:cancellation_dates%0"