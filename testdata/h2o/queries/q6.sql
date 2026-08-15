SELECT id4, id5, MEDIAN(v3) AS median_v3, STDDEV(v3) AS sd_v3 FROM x GROUP BY id4, id5;
