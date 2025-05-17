#!/usr/bin/env python3
import psycopg2
from datetime import datetime, date
#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE

    myHost = ""
    userid = ""
    passwd = ""

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
        # Username is case-insensitive; use LOWER() for uniform comparison
        query = """
        SELECT Username, FirstName, LastName
        FROM Salesperson
        WHERE LOWER(Username) = LOWER(%s)
          AND Password = %s
        """
        cursor.execute(query, (login, password))
        user = cursor.fetchone()

        if user:
            # Return matched user info
            return [user[0], user[1], user[2]]
        else:
            # Return None if username or password is incorrect
            return None

    except psycopg2.Error as e:
        print("Login validation SQL error:", e.pgerror)
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
            cs.MakeCode AS make,
            cs.ModelCode AS model,
            COUNT(*) FILTER (WHERE cs.IsSold = FALSE) AS AvailableUnits,
            COUNT(*) FILTER (WHERE cs.IsSold = TRUE) AS SoldUnits,
            COALESCE(SUM(cs.Price), 0) AS AllPrices,
            COALESCE(SUM(cs.Price) FILTER (WHERE cs.IsSold = TRUE), 0) AS SoldPrices,
            TO_CHAR(MAX(cs.SaleDate), 'DD-MM-YYYY') AS LastPurchasedAt
        FROM CarSales cs
        GROUP BY cs.MakeCode, cs.ModelCode
        ORDER BY cs.MakeCode ASC, cs.ModelCode ASC;
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
                'totalPrices': float(row[4]),
                'soldTotalPrices': float(row[5]),
                'lastPurchaseAt': row[6] if row[6] else 'N/A'
            })

        # Call stored function: calculate_total_sales()
        total_revenue = getTotalSoldRevenue()
        print(f"[DEBUG] Current total sales revenue:${total_revenue:,.2f}")

        return result

    except Exception as e:
        print("Summary function error:", e)
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
            cs.MakeCode,
            cs.ModelCode,
            cs.BuiltYear,
            cs.Odometer,
            cs.Price,
            cs.IsSold,
            TO_CHAR(cs.SaleDate, 'DD-MM-YYYY') AS SaleDate,
            COALESCE(c.FirstName || ' ' || c.LastName, 'N/A') AS Buyer,
            COALESCE(sp.FirstName || ' ' || sp.LastName, 'N/A') AS Salesperson
        FROM CarSales cs
        LEFT JOIN Customer c ON cs.BuyerID = c.CustomerID
        LEFT JOIN Salesperson sp ON cs.SalespersonID = sp.Username
        WHERE 
            (
                LOWER(cs.MakeCode) LIKE %s OR
                LOWER(cs.ModelCode) LIKE %s OR
                LOWER(COALESCE(c.FirstName || ' ' || c.LastName, '')) LIKE %s OR
                LOWER(COALESCE(sp.FirstName || ' ' || sp.LastName, '')) LIKE %s
            )
            AND (
                cs.IsSold = FALSE OR 
                (cs.IsSold = TRUE AND cs.SaleDate >= CURRENT_DATE - INTERVAL '3 years')
            )
        ORDER BY 
            cs.IsSold ASC, 
            cs.SaleDate ASC NULLS FIRST,
            cs.MakeCode ASC,
            cs.ModelCode ASC;
        """

        cursor.execute(query, (keyword, keyword, keyword, keyword))
        rows = cursor.fetchall()

        result = []
        for row in rows:
            result.append({
                'carsale_id': row[0],
                'make': row[1],
                'model': row[2],
                'builtYear': row[3],
                'odometer': row[4],
                'price': float(row[5]),
                'isSold': 'True' if row[6] else 'False',
                'sale_date': row[7],
                'buyer': row[8],
                'salesperson': row[9]
            })

        return result

    except Exception as e:
        print("Car sales query error:", e)
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
def addCarSale(make, model, builtYear, odometer, price):

    if float(price) <= 0:
        print("[ERROR] Price must be positive.")
        return False
    if int(builtYear) > date.today().year:
        print("[ERROR] BuiltYear cannot be in the future.")
        return False
    if int(odometer) < 0:
        print("[ERROR] Odometer cannot be negative.")
        return False

    conn = openConnection()
    cursor = conn.cursor()

    try:
        query = """
        INSERT INTO CarSales (
            MakeCode, ModelCode, BuiltYear, Odometer, Price, IsSold
        ) VALUES (%s, %s, %s, %s, %s, FALSE)
        """
        cursor.execute(query, (make, model, builtYear, odometer, price))
        conn.commit()

        # Call stored function to get brand-wise total sales
        make_sales = getSalesByMake()
        print(f"[DEBUG] Sales totals by brand:{make_sales}")

        return True

    except Exception as e:
        print("Database insert failed:", e)
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
def updateCarSale(carsaleid, customer, salesperson, saledate):
    conn = openConnection()
    cursor = conn.cursor()

    try:
        if isinstance(saledate, str):
            try:
                # Try ISO format: YYYY-MM-DD
                saledate = datetime.strptime(saledate.strip(), "%Y-%m-%d").date()
            except ValueError:
                try:
                    # Try AU format: DD-MM-YYYY
                    saledate = datetime.strptime(saledate.strip(), "%d-%m-%Y").date()
                except ValueError:
                    print(f"[ERROR] Unrecognized date format: {saledate}")
                    saledate = None
        
        # Add future date validation
        if saledate and saledate > date.today():
            print(f"[ERROR] 销售日期 {saledate} 是未来日期，更新被拒绝。")
            return False

        # Step 1: Find BuyerID
        cursor.execute("""
            SELECT CustomerID FROM Customer 
            WHERE LOWER(TRIM(FirstName || ' ' || LastName)) = %s
            LIMIT 1
        """, (customer.lower(),))
        cust_result = cursor.fetchone()

        if not cust_result:
            print(f"[ERROR] Customer '{customer}' not found.")
            return False
        customer_id = cust_result[0]

        # Step 2: Find SalespersonID
        cursor.execute("""
            SELECT UserName FROM Salesperson 
            WHERE LOWER(TRIM(FirstName || ' ' || LastName)) = %s
            LIMIT 1
        """, (salesperson.lower(),))
        sales_result = cursor.fetchone()

        if not sales_result:
            print(f"[ERROR] Salesperson '{salesperson}' not found.")
            return False
        salesperson_id = sales_result[0]

        # Step 3: Build SQL update query
        if saledate:
            query = """
                UPDATE CarSales
                SET 
                    IsSold = TRUE,
                    BuyerID = %s,
                    SalespersonID = %s,
                    SaleDate = %s
                WHERE CarSaleID = %s
            """
            params = (customer_id, salesperson_id, saledate, carsaleid)
        else:
            query = """
                UPDATE CarSales
                SET 
                    IsSold = TRUE,
                    BuyerID = %s,
                    SalespersonID = %s,
                    SaleDate = NULL
                WHERE CarSaleID = %s
            """
            params = (customer_id, salesperson_id, carsaleid)

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            print(f"[ERROR] No car found with CarSaleID = {carsaleid}.")
            conn.rollback()
            return False

        conn.commit()
        print("[INFO] Car sale record updated successfully.")
        return True

    except Exception as e:
        print("[EXCEPTION] Failed to update car sale:", e)
        conn.rollback()
        return False

    finally:
        cursor.close()
        conn.close()

    
# Call function 1: calculate total sales
def getTotalSoldRevenue():
    conn = openConnection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT calculate_total_sales();")
        result = cursor.fetchone()
        return float(result[0]) if result else 0.0
    except Exception as e:
        print("Failed to call calculate_total_sales():", e)
        return 0.0
    finally:
        cursor.close()
        conn.close()

# Call function 2: return total sales by brand
def getSalesByMake():
    conn = openConnection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM get_sales_by_make();")
        rows = cursor.fetchall()
        return [{'make': row[0], 'total': float(row[1])} for row in rows]
    except Exception as e:
        print("Failed to call get_sales_by_make():", e)
        return []
    finally:
        cursor.close()
        conn.close()