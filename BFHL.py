import requests
import sqlite3
from flask import Flask, jsonify

# ----------------------------------------
# 🔁 PART 1: Send SQL query to BFHL webhook
# ----------------------------------------

name = "Sumit Kumar Chaturvedi"
reg_no = "0827CY221062"
email = "sumitkumar220772@acropolis.in"

print(f"👤 Name: {name}")
print(f"🆔 Reg No: {reg_no}")
print(f"📧 Email: {email}")

# Step 1: Generate webhook
response = requests.post(
    "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/PYTHON",
    json={
        "name": name,
        "regNo": reg_no,
        "email": email
    }
)

if response.status_code != 200:
    print("❌ Failed to generate webhook:", response.text)
    exit()

data = response.json()
webhook_url = data["webhook"]
access_token = data["accessToken"]

print(f"✅ Webhook generated.\n🔗 URL: {webhook_url}\n🔐 Token: {access_token[:10]}...")

# Step 2: Final SQL query
final_sql_query = """
SELECT 
    p.AMOUNT AS SALARY,
    CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS NAME,
    TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) AS AGE,
    d.DEPARTMENT_NAME
FROM PAYMENTS p
JOIN EMPLOYEE e ON p.EMP_ID = e.EMP_ID
JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID
WHERE DAY(p.PAYMENT_TIME) != 1
ORDER BY p.AMOUNT DESC
LIMIT 1;
"""

# Step 3: Submit final SQL query
submission = requests.post(
    webhook_url,
    headers={
        "Authorization": access_token,
        "Content-Type": "application/json"
    },
    json={"finalQuery": final_sql_query}
)

if submission.status_code == 200:
    print("✅ SQL query submitted successfully!")
else:
    print("❌ Submission failed:", submission.text)

# ----------------------------------------
# 🌐 PART 2 (Optional): Show result locally
# ----------------------------------------

app = Flask(__name__)

@app.route('/')
def show_result():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    cursor.executescript("""
    CREATE TABLE DEPARTMENT (DEPARTMENT_ID INT, DEPARTMENT_NAME TEXT);
    CREATE TABLE EMPLOYEE (EMP_ID INT, FIRST_NAME TEXT, LAST_NAME TEXT, DOB TEXT, GENDER TEXT, DEPARTMENT INT);
    CREATE TABLE PAYMENTS (PAYMENT_ID INT, EMP_ID INT, AMOUNT REAL, PAYMENT_TIME TEXT);

    INSERT INTO DEPARTMENT VALUES
    (1, 'HR'), (2, 'Finance'), (3, 'Engineering'),
    (4, 'Sales'), (5, 'Marketing'), (6, 'IT');

    INSERT INTO EMPLOYEE VALUES
    (1, 'John', 'Williams', '1980-05-15', 'Male', 3),
    (2, 'Sarah', 'Johnson', '1990-07-20', 'Female', 2),
    (3, 'Michael', 'Smith', '1985-02-10', 'Male', 3),
    (4, 'Emily', 'Brown', '1992-11-30', 'Female', 4),
    (5, 'David', 'Jones', '1988-09-05', 'Male', 5),
    (6, 'Olivia', 'Davis', '1995-04-12', 'Female', 1),
    (7, 'James', 'Wilson', '1983-03-25', 'Male', 6),
    (8, 'Sophia', 'Anderson', '1991-08-17', 'Female', 4),
    (9, 'Liam', 'Miller', '1979-12-01', 'Male', 1),
    (10, 'Emma', 'Taylor', '1993-06-28', 'Female', 5);

    INSERT INTO PAYMENTS VALUES
    (1, 2, 65784.00, '2025-01-01 13:44:12'),
    (2, 4, 62736.00, '2025-01-06 18:36:37'),
    (3, 1, 69437.00, '2025-01-01 10:19:21'),
    (4, 3, 67183.00, '2025-01-02 17:21:57'),
    (5, 2, 66273.00, '2025-02-01 11:49:15'),
    (6, 5, 71475.00, '2025-01-01 07:24:14'),
    (7, 1, 70837.00, '2025-02-03 19:11:31'),
    (8, 6, 69628.00, '2025-01-02 10:41:15'),
    (9, 4, 71876.00, '2025-02-01 12:16:47'),
    (10, 3, 70098.00, '2025-02-03 10:11:17'),
    (11, 6, 67827.00, '2025-02-02 19:21:27'),
    (12, 5, 69871.00, '2025-02-05 17:54:17'),
    (13, 2, 72984.00, '2025-03-05 09:37:35'),
    (14, 1, 67982.00, '2025-03-01 06:09:51'),
    (15, 6, 70198.00, '2025-03-02 10:34:35'),
    (16, 4, 74998.00, '2025-03-02 09:27:26');
    """)

    query = """
    SELECT 
        p.AMOUNT AS SALARY,
        e.FIRST_NAME || ' ' || e.LAST_NAME AS NAME,
        CAST((julianday('now') - julianday(e.DOB)) / 365.25 AS INT) AS AGE,
        d.DEPARTMENT_NAME
    FROM PAYMENTS p
    JOIN EMPLOYEE e ON p.EMP_ID = e.EMP_ID
    JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID
    WHERE strftime('%d', p.PAYMENT_TIME) != '01'
    ORDER BY p.AMOUNT DESC
    LIMIT 1;
    """
    cursor.execute(query)
    row = cursor.fetchone()
    columns = [col[0] for col in cursor.description]
    conn.close()
    return jsonify(dict(zip(columns, row)))

if __name__ == '__main__':
    print("🚀 Flask app running at http://127.0.0.1:5000")
    app.run(debug=True)
