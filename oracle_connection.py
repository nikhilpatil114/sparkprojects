# importing module
import cx_Oracle

# Create a table in Oracle database
try:

    con = cx_Oracle.connect('hr/hr@orclpdb')
    print(con.version)

    # Now execute the sqlquery
    cursor = con.cursor()

    # Creating a table employee
    #cursor.execute("create table memp(empid integer primary key, name varchar2(30), salary number(10, 2))")
    sql_detail="INSERT INTO memp select empid,name,salary from jay"
    cursor.execute(sql_detail)
    con.commit()
    #res = cursor.fetchall()
    #for row in res:
     #   print(row)
    #cursor.execute("INSERT INTO tablet_detail (eno,ename,job,sal)\
     #                   SELECT eno,ename,job,sal\
      #                  FROM jayesh")
    print("Table inserted successfully")

except cx_Oracle.DatabaseError as e:
    print("There is a problem with Oracle", e)

# by writing finally if any error occurs
# then also we can close the all database operation
finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

