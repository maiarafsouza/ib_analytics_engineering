# Teste técnico Analytics Engineerng - Ascential

- python
- great-expectations
- postgresql
- sqlalchemy

To run project:
- Create folder "input_data" with files "Coletas.csv" and "ProdutosVarejos.csv"
- Setup Postgres database
- Run script "0_setup.sql" on postgres server
- Install "requirements" with pip
- Configure ".env" file with variables loaded at app.pg
- Configure great-expectations project with expectations and enpoints described at "great_expectations" folder
- Run "main.py"
- Run "cluster retailer_median_price.ipynb"