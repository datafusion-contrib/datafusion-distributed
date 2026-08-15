SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3;
