### Utility Functions
import pandas as pd
import psycopg2
from psycopg2 import sql, Error
from tqdm import tqdm
from psycopg2.extras import execute_values



from utils import get_db_url


def create_connection(db_url, delete_db=False):
    # In Postgres you will pass in a connection string instead of a filename
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Error as e:
        print("Error connecting to PostgreSQL:", e)
    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    cur = conn.cursor()

    if drop_table_name:
        try:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                sql.Identifier(drop_table_name)
            ))
        except Error as e:
            print(e)

    try:
        cur.execute(create_table_sql)
    except Error as e:
        print(e)


def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)
    return cur.fetchall()


def step1_create_region_table(data_filename, normalized_database_filename):
    region_sql = """
                  CREATE TABLE IF NOT EXISTS Region (
                    RegionID SERIAL PRIMARY KEY,
                    Region VARCHAR NOT NULL UNIQUE
                  );
                  """

    conn = create_connection(normalized_database_filename)
    c = conn.cursor()
    create_table(conn, region_sql, 'region')

    regions = []

    with open(data_filename) as f:
        lines = [line for line in f.read().strip().split('\n') if line]

        for line in lines[1:]:
            region = line.split('\t')[4]
            if region not in regions:
                regions.append(region)

    regions.sort()

    for region in tqdm(regions, desc="Inserting Region Rows"):
        c.execute(
            "INSERT INTO Region (Region) VALUES (%s) ON CONFLICT DO NOTHING",
            (region,)
        )

    conn.commit()
    conn.close()


def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    c = conn.cursor()

    fetch_sql = "SELECT Region, RegionID FROM Region"
    rows = execute_sql_statement(fetch_sql, conn)
    conn.close()

    return dict(rows)


def step3_create_country_table(data_filename, normalized_database_filename):
    country_sql = """
                  CREATE TABLE IF NOT EXISTS Country (
                    CountryID SERIAL PRIMARY KEY,
                    Country VARCHAR NOT NULL,
                    RegionID INTEGER NOT NULL,
                    FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
                  );
                  """

    conn = create_connection(normalized_database_filename)
    c = conn.cursor()
    create_table(conn, country_sql, 'country')

    region_dict = step2_create_region_to_regionid_dictionary(normalized_database_filename)

    countries = []
    with open(data_filename) as f:
        lines = [line for line in f.read().strip().split('\n') if line]

        for line in lines[1:]:
            line = line.split('\t')[3:5]
            country = line[0]
            region = region_dict.get(line[1])
            data = (country, region)
            if data not in countries:
                countries.append(data)

    countries.sort()
    for country in tqdm(countries, desc="Inserting Country Rows"):
        c.execute(
            "INSERT INTO Country (Country, RegionID) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (country[0], country[1])
        )

    conn.commit()
    conn.close()


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    c = conn.cursor()

    fetch_sql = "SELECT Country, CountryID FROM Country"
    rows = execute_sql_statement(fetch_sql, conn)
    conn.close()

    return dict(rows)


def step5_create_customer_table(data_filename, normalized_database_filename):
    customer_sql = """
                  CREATE TABLE Customer (
                    CustomerID SERIAL PRIMARY KEY,
                    FirstName VARCHAR NOT NULL,
                    LastName VARCHAR NOT NULL,
                    Address VARCHAR NOT NULL,
                    City VARCHAR NOT NULL,
                    CountryID INTEGER NOT NULL,
                    FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
                  );
                  """

    conn = create_connection(normalized_database_filename)
    c = conn.cursor()
    create_table(conn, customer_sql, 'customer')

    country_dict = step4_create_country_to_countryid_dictionary(normalized_database_filename)

    custs = []
    with open(data_filename) as f:
        lines = [line for line in f.read().strip().split('\n') if line][1:]

        for line in lines:
            line = line.split('\t')[:4]
            name = line[0].strip().split(' ', 1)
            first = name[0].strip()
            last = name[1].strip()
            address = line[1].strip()
            city = line[2].strip()
            country = line[3].strip()
            cid = country_dict.get(country)
            custs.append([first, last, address, city, cid])

    custs.sort(key=lambda x: x[0])
    for cust in tqdm(custs, desc="Inserting Customer Rows"):
        c.execute(
            "INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES (%s, %s, %s, %s, %s)",
            (cust[0], cust[1], cust[2], cust[3], cust[4])
        )

    conn.commit()
    conn.close()


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    c = conn.cursor()

    fetch_sql = "SELECT CONCAT(FirstName, ' ', LastName) AS Name, CustomerID FROM Customer"
    rows = execute_sql_statement(fetch_sql, conn)
    conn.close()

    return dict(rows)


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    cat_sql = """
                  CREATE TABLE ProductCategory (
                    ProductCategoryID SERIAL PRIMARY KEY,
                    ProductCategory VARCHAR NOT NULL UNIQUE,
                    ProductCategoryDescription VARCHAR NOT NULL
                  );
                  """

    conn = create_connection(normalized_database_filename)
    c = conn.cursor()
    create_table(conn, cat_sql, 'productcategory')

    vals = []
    with open(data_filename) as f:
        lines = [line for line in f.read().strip().split('\n') if line][1:]

        for line in lines:
            line = line.strip().split('\t')[6:8]

            cats = line[0].strip().split(';')
            descs = line[1].strip().split(';')

            for i in range(len(cats)):
                temp = [cats[i].strip(), descs[i].strip()]
                if temp not in vals:
                    vals.append(temp)

    vals.sort()
    for val in tqdm(vals, desc="Inserting ProductCategory Rows"):
        c.execute(
            "INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (val[0], val[1])
        )

    conn.commit()
    conn.close()


def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    c = conn.cursor()

    fetch_sql = "SELECT ProductCategory, ProductCategoryID FROM ProductCategory"
    rows = execute_sql_statement(fetch_sql, conn)
    conn.close()

    return dict(rows)


def step9_create_product_table(data_filename, normalized_database_filename):
    prod_sql = """
                  CREATE TABLE Product (
                    ProductID SERIAL PRIMARY KEY,
                    ProductName VARCHAR NOT NULL,
                    ProductUnitPrice REAL NOT NULL,
                    ProductCategoryID INTEGER NOT NULL,
                    FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
                  );
                  """

    conn = create_connection(normalized_database_filename)
    c = conn.cursor()
    create_table(conn, prod_sql, 'product')

    vals = []
    with open(data_filename) as f:
        lines = [line for line in f.read().strip().split('\n') if line][1:]

        for line in lines:
            line = line.split('\t')
            prods = line[5].strip().split(';')
            cats = line[6].strip().split(';')
            price = line[8].strip().split(';')

            for i in range(len(prods)):
                temp = [prods[i].strip(), price[i].strip(), cats[i].strip()]
                if temp not in vals:
                    vals.append(temp)

    cat_dict = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)

    vals.sort()
    for val in tqdm(vals, desc="Inserting Product Rows"):
        c.execute(
            "INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (val[0], float(val[1]), cat_dict.get(val[2]))
        )

    conn.commit()
    conn.close()


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    c = conn.cursor()

    fetch_sql = "SELECT ProductName, ProductID FROM Product"
    rows = execute_sql_statement(fetch_sql, conn)
    conn.close()

    return dict(rows)


from datetime import datetime

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    order_sql = """
                  CREATE TABLE OrderDetail (
                    OrderID SERIAL PRIMARY KEY,
                    CustomerID INTEGER NOT NULL,
                    ProductID INTEGER NOT NULL,
                    OrderDate DATE NOT NULL,
                    QuantityOrdered INTEGER NOT NULL,
                    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
                    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
                  );
                  """

    conn = create_connection(normalized_database_filename)
    c = conn.cursor()
    create_table(conn, order_sql, 'orderdetail')

    prod_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)
    cust_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)

    vals = []
    with open(data_filename) as f:
        lines = [line for line in f.read().strip().split('\n') if line][1:]

        for line in lines:
            line = line.strip().split('\t')
            cust = cust_dict.get(line[0].strip())
            prod = [prod_dict.get(item) for item in line[5].strip().split(';')]
            date = [datetime.strptime(val, '%Y%m%d').strftime('%Y-%m-%d') for val in line[10].strip().split(';')]
            quant = [int(val) for val in line[9].strip().split(';')]

            for row in zip(prod, date, quant):
                vals.append((cust, row[0], row[1], row[2]))

    print(f"Inserting {len(vals):,} rows into Product Table")
    execute_values(
        c,
        "INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES %s",
        vals,
        page_size=5000
    )

    conn.commit()
    conn.close()

if __name__ == "__main__":
    DATABASE_URL = get_db_url()

    print("Creating tables... \n")
    step1_create_region_table('data.csv', DATABASE_URL)
    print("✅ Region table created \n")
    step3_create_country_table('data.csv', DATABASE_URL)
    print("✅ Country table created\n")
    step5_create_customer_table('data.csv', DATABASE_URL)
    print("✅ Customer table created\n")
    step7_create_productcategory_table('data.csv', DATABASE_URL)
    print("✅ ProductCategory table created\n")
    step9_create_product_table('data.csv', DATABASE_URL)
    print("✅ Product table created\n")
    step11_create_orderdetail_table('data.csv', DATABASE_URL)
    print("✅ Order table created\n")

    print("✅ Database creation complete!")