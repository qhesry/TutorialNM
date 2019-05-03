
drop table if exists lvl_receiver_lvl_day_zone;
create table lvl_receiver_lvl_day_zone as select 
        r.the_geom the_geom,
        r.id id,
           10.*log10(
       power(10,10.*log10(sum(power(10,(s.db_m_d63+l.att63)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d125+l.att125)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d250+l.att250)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d500+l.att500)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d1000+l.att1000)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d2000+l.att2000)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d4000+l.att4000)/10)))/10)
        +power(10,10.*log10(sum(power(10,(s.db_m_d8000+l.att8000)/10)))/10)
       ) db      
        from receiver_lvl_day_zone l, receivers2 r,  roads_src_zone s
        where l.idsource=s.id and r.pk2 = l.idrecepteur and s.db_m_d63+l.att63>0
        group by r.id, r.the_geom;
