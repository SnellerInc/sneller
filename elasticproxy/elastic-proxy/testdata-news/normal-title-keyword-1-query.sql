WITH
  "$source" AS
    (SELECT *
     FROM "test"."news" AS "$source"
     WHERE ("$source"."title" = 'Biden')
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