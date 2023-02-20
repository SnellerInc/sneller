-- See https://github.com/SnellerInc/sneller-core/issues/2480
WITH
  "$source" AS
    (SELECT *
     FROM "table" AS "$source"
    ),

  "$bucket:regional%0" AS
    (SELECT "$source"."region" AS "$key:regional%0",
            COUNT(*) AS "$doc_count",
            AVG("$source"."duration") AS "avg_duration"
     FROM "$source"
     GROUP BY "$source"."region"
     ORDER BY "$doc_count" DESC
     LIMIT 10
    ),

  "$bucket:regional:dest%0" AS
    (SELECT "$source"."region" AS "$key:regional%0",
            "$source"."dest_ip" AS "$key:regional:dest%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     WHERE ("$source"."region" IN (SELECT "$selection"."$key:regional%0"
     FROM "$bucket:regional%0" AS "$selection"))
     GROUP BY "$source"."region",
              "$source"."dest_ip"
     HAVING (ROW_NUMBER() OVER (PARTITION BY "$source"."region" ORDER BY COUNT(*) DESC) <= 10)
     ORDER BY "$doc_count" DESC
    ),

  "$bucket:regional:src%0" AS
    (SELECT "$source"."region" AS "$key:regional%0",
            "$source"."source_ip" AS "$key:regional:src%0",
            COUNT(*) AS "$doc_count",
            COUNT(DISTINCT "$source"."host") AS "hosts"
     FROM "$source"
     WHERE ("$source"."region" IN (SELECT "$selection"."$key:regional%0"
     FROM "$bucket:regional%0" AS "$selection"))
     GROUP BY "$source"."region",
              "$source"."source_ip"
     HAVING (ROW_NUMBER() OVER (PARTITION BY "$source"."region" ORDER BY COUNT(*) DESC) <= 10)
     ORDER BY "$doc_count" DESC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:regional%0"
  ) AS "$bucket:regional%0",

  (SELECT *
   FROM "$bucket:regional:dest%0"
  ) AS "$bucket:regional:dest%0",

  (SELECT *
   FROM "$bucket:regional:src%0"
  ) AS "$bucket:regional:src%0"