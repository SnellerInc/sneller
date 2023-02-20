WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (LOWER("$source"."Carrier") ~ '(^|[ \t])Kibana.*([ \t]|$)')
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count"