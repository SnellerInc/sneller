WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-06-05T00:00:00Z`) AND ("$source"."timestamp" < `2022-06-06T00:00:00Z`))
    ),

  "$bucket:region%0" AS
    (SELECT "$source"."region" AS "$key:region%0",
            COUNT(*) AS "$doc_count",
            COUNT(DISTINCT "$source"."source_ip") AS "unique_ips"
     FROM "$source"
     GROUP BY "$source"."region"
     ORDER BY "$doc_count" DESC
     LIMIT 10
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$source"
   LIMIT 3
  ) AS "$hits",

  (SELECT *
   FROM "$bucket:region%0"
  ) AS "$bucket:region%0"