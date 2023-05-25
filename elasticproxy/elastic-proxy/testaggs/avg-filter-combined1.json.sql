WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:%0" AS
    (SELECT AVG("$source"."price") AS "avg_overall_price",
            COUNT(DISTINCT "$source"."type") AS "total_types"
     FROM "$source"
    ),

  "$bucket:t_shirts%0" AS
    (SELECT COUNT(*) AS "$doc_count",
            AVG("$source"."price") AS "avg_price"
     FROM "$source"
     WHERE ("$source"."type" = 't-shirt')
     ORDER BY "$doc_count" DESC
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
   FROM "$bucket:%0"
  ) AS "$bucket:%0",

  (SELECT *
   FROM "$bucket:t_shirts%0"
  ) AS "$bucket:t_shirts%0"