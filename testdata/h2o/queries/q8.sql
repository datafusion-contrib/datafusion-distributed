SELECT id6, largest2_v3 FROM (SELECT id6, v3 AS largest2_v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 FROM x WHERE v3 IS NOT NULL) sub_query WHERE order_v3 <= 2;
