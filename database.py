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

        # 调用存储函数：calculate_total_sales()
        total_revenue = getTotalSoldRevenue()
        print(f"[DEBUG] 当前总销售额为：${total_revenue:,.2f}")

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
def addCarSale(make, model, builtYear, odometer, price):
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

        # 调用 stored function 查看品牌销售额
        make_sales = getSalesByMake()
        print(f"[DEBUG] 当前各品牌销售额为：{make_sales}")

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
def updateCarSale(carsaleid, customer, salesperson, saledate):
    conn = openConnection()
    cursor = conn.cursor()

    try:
        # 🚨 加上未来日期校验
        if saledate and saledate > date.today():
            print(f"[ERROR] 销售日期 {saledate} 是未来日期，更新被拒绝。")
            return False

        # Step 1: 查找 BuyerID
        cursor.execute("""
            SELECT CustomerID FROM Customer 
            WHERE TRIM(FirstName || ' ' || LastName) = %s
            LIMIT 1
        """, (customer,))
        cust_result = cursor.fetchone()

        if not cust_result:
            print(f"[ERROR] 客户 {customer} 不存在。")
            return False
        customer_id = cust_result[0]

        # Step 2: 查找 SalespersonID
        cursor.execute("""
            SELECT UserName FROM Salesperson 
            WHERE TRIM(FirstName || ' ' || LastName) = %s
            LIMIT 1
        """, (salesperson,))
        sales_result = cursor.fetchone()

        if not sales_result:
            print(f"[ERROR] 销售员 {salesperson} 不存在。")
            return False
        salesperson_id = sales_result[0]

        # Step 3: 构建 SQL 动态更新语句
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
            print(f"[ERROR] 未找到 CarSaleID = {carsaleid} 的记录。")
            conn.rollback()
            return False

        conn.commit()
        print("[INFO] 销售记录更新成功。")
        return True

    except Exception as e:
        print("[EXCEPTION] 销售记录更新失败：", e)
        conn.rollback()
        return False

    finally:
        cursor.close()
        conn.close()

    
    # 调用函数 1：计算总销售额
def getTotalSoldRevenue():
    conn = openConnection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT calculate_total_sales();")
        result = cursor.fetchone()
        return float(result[0]) if result else 0.0
    except Exception as e:
        print("调用 calculate_total_sales() 失败：", e)
        return 0.0
    finally:
        cursor.close()
        conn.close()

# 调用函数 2：按品牌返回总销售额
def getSalesByMake():
    conn = openConnection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM get_sales_by_make();")
        rows = cursor.fetchall()
        return [{'make': row[0], 'total': float(row[1])} for row in rows]
    except Exception as e:
        print("调用 get_sales_by_make() 失败：", e)
        return []
    finally:
        cursor.close()
        conn.close()