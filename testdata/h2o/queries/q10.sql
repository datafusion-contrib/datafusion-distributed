SELECT id1, id2, id3, id4, id5, id6, SUM(v3) AS v3, COUNT(*) AS count FROM x GROUP BY id1, id2, id3, id4, id5, id6;
