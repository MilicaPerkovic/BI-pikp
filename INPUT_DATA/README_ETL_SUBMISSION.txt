ETL oddaja - Ekipa 6

Kaj je pripravljeno:
1) Transformacije dimenzij (modelna verzija):
   - transformations_model/tr_dim_time.py
   - transformations_model/tr_dim_country.py
   - transformations_model/tr_dim_region_slovenia.py
   - transformations_model/tr_dim_municipality.py
   - transformations_model/run_all_model.py

2) Vhodne datoteke:
   - 2164466S_20260315-191917.xlsx
   - 2164466S_20260315-191956.xlsx
   - tec00114_page_spreadsheet.xlsx

3) Izhodne dimenzije (CSV):
   - output_dimensions_model/dim_time.csv
   - output_dimensions_model/dim_country.csv
   - output_dimensions_model/dim_region_slovenia.csv
   - output_dimensions_model/dim_municipality.csv

Kaj je treba še ročno dodati pred oddajo:
1) Zaslonske posnetke transformacij v mapo screenshots/.
2) Po potrebi dopolniti dim_country.csv za manjkajoce PPS/GDP vrednosti (drzave brez podatkov v viru Eurostat ostanejo prazne).

Predlagan zagon transformacij:
- ./.venv/bin/python transformations_model/run_all_model.py

Predlagano ime ZIP datoteke:
- Ekipa6_ETL_Dim.zip
