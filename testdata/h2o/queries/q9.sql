SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4;
