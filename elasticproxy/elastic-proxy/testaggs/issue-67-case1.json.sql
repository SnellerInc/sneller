WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:events_over_time%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",86400) AS "$key:events_over_time%0",
            COUNT(*) AS "$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."sessionSummaryInfo"."threatInfo"."ipsThreatCount" IS NOT MISSING)) AS "aggs0:$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."sessionSummaryInfo"."threatInfo"."wafThreatCount" IS NOT MISSING)) AS "aggs1:$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."sessionSummaryInfo"."threatInfo"."l7DOSThreatCount" IS NOT MISSING)) AS "aggs2:$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."sessionSummaryInfo"."threatInfo"."urlFilteringDenyCount" IS NOT MISSING)) AS "aggs3:$doc_count",
            COUNT(*) FILTER (WHERE ("$source"."sessionSummaryInfo"."threatInfo"."tlsHandshakeFailureCount" IS NOT MISSING)) AS "aggs4:$doc_count"
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