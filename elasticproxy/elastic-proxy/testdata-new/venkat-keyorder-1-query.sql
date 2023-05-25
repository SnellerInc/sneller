WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:0%0" AS
    (SELECT "$source"."OriginCountry" AS "$key:0%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     GROUP BY "$source"."OriginCountry"
     ORDER BY "$key:0%0" ASC
     LIMIT 5
    ),

  "$bucket:0:1%0" AS
    (SELECT "$source"."OriginCountry" AS "$key:0%0",
            "$source"."DestCountry" AS "$key:0:1%0",
            COUNT(*) AS "$doc_count"
     FROM "$source"
     WHERE ("$source"."OriginCountry" IN (SELECT "$selection"."$key:0%0"
     FROM "$bucket:0%0" AS "$selection"))
     GROUP BY "$source"."OriginCountry",
              "$source"."DestCountry"
     HAVING (ROW_NUMBER() OVER (PARTITION BY "$source"."OriginCountry" ORDER BY "$source"."DestCountry" ASC) <= 3)
     ORDER BY "$key:0:1%0" ASC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:0%0"
  ) AS "$bucket:0%0",

  (SELECT *
   FROM "$bucket:0:1%0"
  ) AS "$bucket:0:1%0"