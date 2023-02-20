WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:events_over_time%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",86400) AS "$key:events_over_time%0",
            COUNT(*) AS "$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."DestCountry" = 'AU')) AS "aggs0:$doc_count"
     FROM "$source"
     GROUP BY TIME_BUCKET("$source"."timestamp",86400)
     ORDER BY "$key:events_over_time%0" ASC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:events_over_time%0"
  ) AS "$bucket:events_over_time%0"