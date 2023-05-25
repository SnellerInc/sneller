WITH
  "$source" AS
    (SELECT *
     FROM "test"."news" AS "$source"
     WHERE (LOWER("$source"."title") = 'biden')
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$source"
   ORDER BY "$source"."published_at" ASC
   LIMIT 10
  ) AS "$hits"