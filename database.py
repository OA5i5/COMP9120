#!/usr/bin/env python3
import psycopg2
from datetime import datetime
#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE

    myHost = "awsprddbs4836.shared.sydney.edu.au"
    userid = "y25s1c9120_rzha0623"
    passwd = "ac619uu"
    
    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate salesperson based on username and password
'''
def checkLogin(login, password):
    conn = openConnection()
    cursor = conn.cursor()

    try:
        # 用户名不区分大小写，使用 LOWER 统一小写比较
        query = """
        SELECT Username, FirstName, LastName
        FROM Salesperson
        WHERE LOWER(Username) = LOWER(%s)
          AND Password = %s
        """
        cursor.execute(query, (login, password))
        user = cursor.fetchone()

        if user:
            # 返回查找到的用户信息
            return [user[0], user[1], user[2]]
        else:
            # 若用户名密码错误则返回None
            return None

    except psycopg2.Error as e:
        print("登录验证SQL执行错误:", e.pgerror)
        return None

    finally:
        cursor.close()
        conn.close()
    


"""
    Retrieves the summary of car sales.

    This method fetches the summary of car sales from the database and returns it 
    as a collection of summary objects. Each summary contains key information 
    about a particular car sale.

    :return: A list of car sale summaries.
"""
def getCarSalesSummary():
    conn = openConnection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT
            Make,
            Model,
            COUNT(*) FILTER (WHERE IsSold = FALSE) AS AvailableUnits,
            COUNT(*) FILTER (WHERE IsSold = TRUE) AS SoldUnits,
            COALESCE(SUM(cs.DiscountPrice), 0) AS TotalSales,
            TO_CHAR(MAX(cs.SaleDate), 'DD-MM-YYYY') AS LastPurchasedAt
        FROM Vehicle v
        LEFT JOIN CarSale cs ON v.VIN = cs.VIN
        GROUP BY Make, Model
        ORDER BY Make ASC, Model ASC;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        result = []
        for row in rows:
            result.append({
                'make': row[0],
                'model': row[1],
                'availableUnits': row[2],
                'soldUnits': row[3],
                'soldTotalPrices': float(row[4]),
                'lastPurchaseAt': row[5] if row[5] else 'N/A'
            })

        return result

    except Exception as e:
        print("汇总函数错误:", e)
        return []
    finally:
        cursor.close()
        conn.close()

"""
    Finds car sales based on the provided search string.

    This method searches the database for car sales that match the provided search 
    string. See assignment description for search specification

    :param search_string: The search string to use for finding car sales in the database.
    :return: A list of car sales matching the search string.
"""
def findCarSales(searchString):
    conn = openConnection()
    cursor = conn.cursor()

    try:
        keyword = f"%{searchString.lower()}%"

        query = """
        SELECT 
            cs.CarSaleID,
            v.Make,
            v.Model,
            v.BuiltYear,
            v.Odometer,
            v.Price,
            v.IsSold,
            TO_CHAR(cs.SaleDate, 'DD-MM-YYYY') AS SaleDate,
            c.FirstName || ' ' || c.LastName AS Buyer,
            s.FirstName || ' ' || s.LastName AS Salesperson
        FROM CarSale cs
        JOIN Vehicle v ON cs.VIN = v.VIN
        JOIN Customer c ON cs.CustomerID = c.CustomerID
        JOIN Salesperson s ON cs.SalespersonID = s.StaffID
        WHERE 
            LOWER(v.Make) LIKE %s OR
            LOWER(v.Model) LIKE %s OR
            LOWER(c.FirstName || ' ' || c.LastName) LIKE %s OR
            LOWER(s.FirstName || ' ' || s.LastName) LIKE %s
        ORDER BY cs.SaleDate DESC;
        """

        cursor.execute(query, (keyword, keyword, keyword, keyword))
        results = cursor.fetchall()

        sales = []
        for row in results:
            sales.append({
                'carsale_id': row[0],
                'make': row[1],
                'model': row[2],
                'builtYear': row[3],
                'odometer': row[4],
                'price': float(row[5]),
                'isSold': 'Yes' if row[6] else 'No',
                'sale_date': row[7],
                'buyer': row[8],
                'salesperson': row[9]
            })

        return sales

    except Exception as e:
        print("销售记录查询错误:", e)
        return []

    finally:
        cursor.close()
        conn.close()

"""
    Adds a new car sale to the database.

    This method accepts a CarSale object, which contains all the necessary details 
    for a new car sale. It inserts the data into the database and returns a confirmation 
    of the operation.

    :param car_sale: The CarSale object to be added to the database.
    :return: A boolean indicating if the operation was successful or not.
"""
def addCarSale(make, model, builtYear, odometer, price, colour="Unspecified", transmission="Automatic", description="Added via web"):
    conn = openConnection()
    cursor = conn.cursor()

    try:
        vin = str(abs(hash(f"{make}{model}{builtYear}{odometer}{price}{datetime.now()}")))[:15]

        cursor.execute("""
            INSERT INTO Vehicle (
                VIN, Make, Model, BuiltYear, Odometer, Colour, TransmissionType,
                Price, IsSold, Description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s)
        """, (vin, make, model, builtYear, odometer, colour, transmission, price, description))

        cursor.execute("INSERT INTO NewVehicle (VIN) VALUES (%s)", (vin,))
        conn.commit()
        return True

    except Exception as e:
        print("数据库插入失败：", e)
        conn.rollback()
        return False

    finally:
        cursor.close()
        conn.close()

"""
    Updates an existing car sale in the database.

    This method updates the details of a specific car sale in the database, ensuring
    that all fields of the CarSale object are modified correctly. It assumes that 
    the car sale to be updated already exists.

    :param car_sale: The CarSale object containing updated details for the car sale.
    :return: A boolean indicating whether the update was successful or not.
"""
def updateCarSale(carsaleid, customer, salesperosn, saledate):
    return
