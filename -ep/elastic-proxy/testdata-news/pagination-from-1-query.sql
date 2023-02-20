WITH
  "$source" AS
    (SELECT *
     FROM "news" AS "$source"
     WHERE ("$source"."title" ~ '(^|[ \t])(?i)Biden([ \t]|$)')
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$source"
   ORDER BY "$source"."published_at" ASC
   LIMIT 2
   OFFSET 2
  ) AS "$hits"