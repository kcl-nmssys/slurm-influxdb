CREATE RETENTION POLICY "1week" ON <database> DURATION 7d REPLICATION 1

CREATE CONTINUOUS QUERY "cq_time_pending_1day_partition" ON <database>
BEGIN
  SELECT MAX("jobs_time_pending") INTO "partition_jobs_time_pending_daily" FROM "1week"."partition_jobs_time_pending" GROUP BY time(1d),*
END

CREATE CONTINUOUS QUERY "cq_time_pending_1day_group" ON <database>
BEGIN
  SELECT MAX("jobs_time_pending") INTO "group_jobs_time_pending_daily" FROM "1week"."group_jobs_time_pending" GROUP BY time(1d),*
END


CREATE CONTINUOUS QUERY "cq_time_pending_1week_partition" ON <database>
BEGIN
  SELECT MAX("jobs_time_pending") INTO "partition_jobs_time_pending_weekly" FROM "partition_jobs_time_pending_daily" GROUP BY time(7d),*
END

CREATE CONTINUOUS QUERY "cq_time_pending_1week_group" ON <database>
BEGIN
  SELECT MAX("jobs_time_pending") INTO "group_jobs_time_pending_weekly" FROM "group_jobs_time_pending_daily" GROUP BY time(7d),*
END