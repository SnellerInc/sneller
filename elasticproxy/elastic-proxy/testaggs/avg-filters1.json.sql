WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:t_shirts:errors%0" AS
    (SELECT COUNT(*) AS "$doc_count",
            AVG("$source"."price") AS "avg_price"
     FROM "$source"
     WHERE ("$source"."body" = 'error')
     ORDER BY "$doc_count" DESC
    ),

  "$bucket:t_shirts:warnings%0" AS
    (SELECT COUNT(*) AS "$doc_count",
            AVG("$source"."price") AS "avg_price"
     FROM "$source"
     WHERE ("$source"."body" = 'warning')
     ORDER BY "$doc_count" DESC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:t_shirts:errors%0"
  ) AS "$bucket:t_shirts:errors%0",

  (SELECT *
   FROM "$bucket:t_shirts:warnings%0"
  ) AS "$bucket:t_shirts:warnings%0"