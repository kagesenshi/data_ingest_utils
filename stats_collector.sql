create table if not exists default.stats_collector (
      sessionid string, 
      ts timestamp,
      hostname string, 
      cpu float, 
      ram float,
      net_bytes_sent int, 
      net_bytes_recv int);
