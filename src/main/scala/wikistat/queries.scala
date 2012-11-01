package edu.mit.cryptdb.wikistat

object Queries {
  val q1 = """
 select count(distinct page_id)
 from pagestat join datesinfo di on ( di.id=date_id )
 where
  di.calmonth=6 and di.calyear=2009;
"""

  val q2 = """
 select caldate, sum(page_count)
 from pagestat join datesinfo di on ( di.id=date_id )
 where
  di.calmonth=7 and di.calyear=2009
 group by caldate
 order by caldate;
"""

  val q3 = """
 select project, sum(page_count) as sm
 from pagestat
   join datesinfo di on ( di.id=date_id )
   join projects p on  (p.id=project_id )
 where
  di.calmonth=7 and di.calyear=2009
 group by project
 order by sm desc
 limit 20;
"""

  val q4 = """
 select project_id, sum(page_count) as sm
 from pagestat
 group by project_id;
"""

  val q5 = """
 select dayhour, sum(page_count) as sm
 from pagestat
   join datesinfo di on ( di.id=date_id )
   join projects p on  (p.id=project_id )
 where di.caldate='2009-07-21' and project='es'
 group by dayhour;
"""
  val q6 = """
 select project
 from projects left outer join pagestat on (pagestat.project_id = projects.id)
 where pagestat.project_id is null
 order by project;
"""

  val q7 = """
 select project
 from projects left outer join
   (select distinct project_id pid from pagestat where date_id IN
    (select id from datesinfo where calmonth=7 and calyear=2009 and calday=14)) t1
  on (projects.id=pid)
 where pid is null
 order by project;
"""

  val q8 = """
 select dayofweek, sum(page_count) as sm
 from pagestat
   join datesinfo di on ( di.id=date_id )
   join projects p on  (p.id=project_id )
 where di.calmonth=6 and di.calyear=2009 and project='de'
 group by dayofweek;
"""

  val q9 = """
 select page, sum(page_count) as sm
 from pagestat
   join datesinfo di on ( di.id=date_id )
   join projects p on  (p.id=project_id )
   join pages pp on (pp.id=page_id)
 where di.calmonth=6 and di.calyear=2009 and di.calday=18
   and project='de'
 group by page
 order by sm desc
 limit 50;
"""

  val q10 = """
 select page_id, max(page_count) as mx
 from pagestat
   join datesinfo di on ( di.id=date_id )
 where di.calmonth=6 and di.calyear=2009 and di.calday=18
 group by page_id
 order by mx desc
 limit 50;
"""

  val q11 = """
 select page_id, max(page_count) as mx
 from pagestat
   join datesinfo di on ( di.id=date_id )
 where di.calmonth=7 and di.calyear=2009 and di.calday=18
 group by page_id
 having max(page_count) > 10000
 limit 50;
"""

  val q12 = """
 select page, mx
 from pages join (select page_id, max(page_count) as mx
        from pagestat
        join datesinfo di on ( di.id=date_id )
        where di.calmonth=7 and di.calyear=2009 and di.calday=18
        group by page_id
        having max(page_count) > 10000  limit 50) t
  on (t.page_id=pages.id);
"""

  val q13 = """
 select page from pages join
  (select t1.page_id from
    (select p1.page_id,sum(page_count) as sm from pagestat p1 join datesinfo di1 on ( di1.id=p1.date_id )
     where di1.calmonth=6 and di1.calyear=2009 and di1.calday=18 and p1.project_id=193
     group by p1.page_id
     having sum(page_count)>1000)  t1
    left outer join
    (select p2.page_id from pagestat p2 join datesinfo di2 on ( di2.id=p2.date_id )
     where di2.calmonth=6 and di2.calyear=2009 and di2.calday=19 and p2.project_id=193) t2
    on (t2.page_id=t1.page_id)  where t2.page_id is null) bq
 on (pages.id=page_id);
"""

  val q14 = """
select caldate, sum(page_count) from pagestat join datesinfo on ( pagestat.date_id = datesinfo.id)
   join pages on (pagestat.page_id=pages.id) where page='Yugopolis' and calmonth=7 and calyear=2009 group by caldate order by caldate;
"""

  val q15 = """
  select project, avg(page_count) from pagestat join datesinfo on ( pagestat.date_id = datesinfo.id)
   join projects on (pagestat.project_id=projects.id) where calmonth=7 and calyear=2009 group by project;
"""

  val q16 = """
  select page_id,  count(distinct project_id) from pagestat join datesinfo di on ( di.id=date_id )
   where di.calmonth=7 and di.calyear=2009 group by page_id having count(distinct project_id) > 10 limit 100;
"""

  val q17 = """
 select ps1.page_id, ps1.page_count
 from pagestat ps1
  join datesinfo di1 on ( ps1.date_id = di1.id)
  join pagestat ps2 on (ps2.page_id=ps1.page_id)
  join  datesinfo di2 on ( ps2.date_id = di2.id)
where di1.caldate='2009-07-20' and di1.dayhour=14
  and di2.caldate='2009-07-20' and di2.dayhour=15
  and ps1.project_id=193 and ps2.project_id=193
  and ps1.page_count > 2 * ps2.page_count
  and ps1.page_count>=1000 limit 100;
"""

  val q18 = """
 select project, sum(page_count) as sm
 from pagestat_daily
  join datesinfo dstart on ( date_id_from=dstart.id)
  join datesinfo dend on (date_id_to=dend.id)
  join projects on (project_id=projects.id)
 where dstart.caldate>='2009-06-15' and dend.caldate <= '2009-06-20'
 group by project having sum(page_count) > 1000
 order by sm desc;
"""

  val q19 = """
 select page, sm from pages,
 ( select page_id, sum(page_count) as sm
   from pagestat_daily
    join datesinfo dstart on ( date_id_from=dstart.id)
    join datesinfo dend on (date_id_to=dend.id)
  where dstart.caldate>='2009-06-15' and dend.caldate <= '2009-06-20'
 group by page_id ) dt
 where pages.id=dt.page_id order by sm desc limit 100;
"""

  val q20 = """
 select ps.page_id, max(ps.page_count/pd.page_count) as max_daily_spikiness
 from pagestat ps
 inner join pagestat_daily pd on ps.page_id=pd.page_id
   and ps.date_id between pd.date_id_from and pd.date_id_to
 group by ps.page_id
 order by max_daily_spikiness desc
 limit 10;
"""

  val AllQueries = Seq(q1, q2, q3, q4, q5, /*q6,*/ /*q7,*/ q8, q9, q10, q11, q12, /*q13,*/ q14, q15, q16, q17, q18, q19, q20)
}
