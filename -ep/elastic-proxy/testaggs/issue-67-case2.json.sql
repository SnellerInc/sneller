WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:events_over_time%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",86400) AS "$key:events_over_time%0",
            COUNT(*) AS "$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."ip" IS NOT MISSING)) AS "ips:$doc_count",
            AVG("$source"."count") AS "avg_count"
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