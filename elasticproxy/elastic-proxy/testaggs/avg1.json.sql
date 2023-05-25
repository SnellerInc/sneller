WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:%0" AS
    (SELECT AVG("$source"."price") AS "avg_price",
            FALSE AS "$dummy$"
     FROM "$source"
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:%0"
  ) AS "$bucket:%0"