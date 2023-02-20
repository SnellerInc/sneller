WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
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
   FROM "$bucket:t_shirts%0"
  ) AS "$bucket:t_shirts%0"