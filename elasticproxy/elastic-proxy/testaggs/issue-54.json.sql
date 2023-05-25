WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:name%0" AS
    (SELECT [(
      SELECT "$source"."timestamp" AS "$key:name%0",
             COUNT(*) AS "$doc_count"
      FROM "$source"
      GROUP BY "$source"."timestamp"
      ORDER BY "$key:name%0" ASC
      LIMIT 1
    )])

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:name%0"
  ) AS "$bucket:name%0"