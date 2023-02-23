dictionary = {
  'schema': 'wkdansqls',
  'table': 'nps_summary',
  'primary_key': 'date',
  'main_sql': """
     SELECT DATE(created_at) AS date
          , SUM(CASE WHEN score >= 9 THEN 1
                     WHEN score <= 6 THEN -1 END)::FLOAT
            / COUNT(1)
     FROM wkdansqls.nps
     GROUP BY date;
     """,
  'input_test': [
         {
             'sql': "SELECT COUNT(1) FROM wkdansqls.nps;",
             'count': 150000
         }
     ],
  'output_test': [
         {
             'sql': "SELECT COUNT(1) FROM wkdansqls.stage;",
             'count': 12
         }
     ]
}
