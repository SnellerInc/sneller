WITH
  "$source" AS
    (SELECT *
     FROM "news" AS "$source"
     WHERE ("$source"."title" ~ '^(?i)biden.*$')
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