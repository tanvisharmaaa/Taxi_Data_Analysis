# NYC Taxi Data Analysis

##  Overview
This project involves analyzing NYC taxi ride data using PySpark to gain insights into taxi and driver performance. The assignment consists of four main tasks, ranging from identifying top-performing taxis and drivers to advanced analytics involving profit patterns and payment methods.

---

##  Tasks

###  Task 1 - Top 10 Active Taxis
Compute the top 10 taxis that have had the **largest number of unique drivers**.

> 📈 Output:  
`(medallion, number_of_drivers)`

---

###  Task 2 - Top 10 Best Drivers
Identify the top 10 drivers based on **average money earned per minute** spent carrying a customer.

> 📈 Output:  
`(hack_license, avg_money_per_minute)`

---

###  Task 3 - Best Time of Day to Drive
Determine which **hour of the day** offers the highest **profit per mile**. Profit is based on surcharge (excluding tip), and the distance traveled.

> 🧮 Formula:  
`Profit Ratio = Surcharge / Trip Distance (in miles)`

> 📈 Output:  
`(hour_of_day, profit_ratio)`

---

###  Task 4 – Advanced Analytics
#### 🔹 Payment Method by Hour
Calculate the **percentage of card vs. cash payments** for each hour of the day and in total.

> 📈 Output:  
`(hour_of_day, percent_card)`

#### 🔹 Driver Efficiency
Compute the **top 10 most efficient drivers** based on **money earned per mile** (including tips).

> 📈 Output:  
`(hack_license, avg_money_per_mile)`


