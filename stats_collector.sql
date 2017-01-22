create table if not exists default.stats_collector (
      sessionid string, 
      ts int,
      hostname string, 
      cpu float, 
      ram float,
      process_count int,
      net_bytes_sent int, 
      net_bytes_recv int);
