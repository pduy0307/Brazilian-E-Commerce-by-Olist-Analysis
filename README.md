# Brazilian-E-Commerce-by-Olist-Analysis

### Project Overview
The Olist dataset, sourced from Kaggle, contains comprehensive data on sales transactions, customer demographics, product details, and more from a Brazilian e-commerce platform. This dataset provides a rich source for analyzing e-commerce activities, customer behavior, and sales trends. The following executive summary outlines key insights derived from the data, focusing on sales performance, order fulfilment, customer segmentation, and payment methods.

### Tools Used
![Extract (2)](https://github.com/user-attachments/assets/1e2d467a-b3c2-46a2-8e1c-0e02fd621d2b)


- Apache Airflow: Extract data source using Kaggle API. then tranform and load it into PostgreSQL database by using Docker container.
- PostgreSQL: Data Analysis
- Apache Superset: Creating Reports and Dashboards

# Deep Dive
- ETL Process: Create dags to pull all the datasets, create tables corresponding to each retrieved data table then store in PostgresSQL

![image](https://github.com/user-attachments/assets/dc354a89-1d8c-459e-ac1a-43420564501b)

- Connect the database via Dbeaver

## Data Analysis
- The data analysis is conducted using SQL queries in PostgreSQL and visualized through Apache Superset dashboards. Key insights include:
  - Sales trends and performance over time
  - Customer Analysis
  - Popular product categories and regional preferences
  - Order fulfillment performance

## Visualization
Link to interactive [Dashboard](https://f1f1-113-173-157-136.ngrok-free.app/superset/dashboard/15f31c5d-b975-4da6-99e8-f1d9e0abdbaf/?native_filters_key=GKu3c0AztulUlLm2cuTatRa2JD32eCZZsPr77gSt9v-n9DjnwIqFf6nr1rhkIRFc)

## Key Insights 
- ‌Olist E-commerce Store generated a total sales of 16 million Brazilian real(R$16,008,872) ≈ 2,873,711 USD within the 3-year period in review. During this, I found that in November 2017 had the highest sales, especially in 24th November 2017 and surprisingly this was on **Black Friday** which was a very common reason why sales grow.
  ![image](https://github.com/user-attachments/assets/5a66b42a-e9bf-454c-8d59-986abd481dc5)

- Users tend to buy more on weekdays, especially the hours from 2:00 p.m. to 4:00 p.m. and 9:00 p.m record high sale number
  ![image](https://github.com/user-attachments/assets/22169703-ed55-4775-a77e-060f81de899a)

- People in Brazil tend to use their Credit card as their payment method (Credit card accounts nearly for 74% of total payment medthod, which create total sales of 2,251,235 USD). The reason may be due to credit card ability: Installment payments or Buy Now, Pay Later
  ![image](https://github.com/user-attachments/assets/2e82234c-85b1-4dd6-87d5-652226e3ed86)

- ‌Olist E-commerce Store has 97% Delivery Success Rate for all placed orders. The average time for an order to reach the customer is 10 days, Brazil has 26 States and only São Paulo has the average time lower than 10 days (8.79 days). It indicates Olist has a well-established logistics and Order fulfillment system in place, but not optimal in terms of shipping time.
  ![image](https://github.com/user-attachments/assets/1b01c671-9b42-4661-b7f6-3b45fdd02137)

- 36.6 % of total customers is Loyal Customers, with a Repeat Customer Rate at 25.8 %. Olist has a good revenue due to Loyal Customers. However, the next customer segmentation that generate high number of Orders and Sales is At risk customers. They contribute 18.4% and 27% number of orders and total sales respectively
  ![image](https://github.com/user-attachments/assets/304a14e1-c140-43a8-9511-6689b526cdcd)
  ![image](https://github.com/user-attachments/assets/f7208b63-7bee-4428-a66e-111083fd0857)
  ![image](https://github.com/user-attachments/assets/51c532d3-e30d-4f38-ab8f-1d1599b7ee9d)

## Recommendations
**- Maximize sales during peak periods**
  - Leverage Black Friday and Holiday Seasons: Since November 2017, particularly Black Friday, has seen the largest sales volume, Olist should continue to profit on such peak periods. During certain times, enhanced marketing campaigns, special promotions, and discounts might help to drive sales even higher. Prepare for more traffic anđ ensure that the platform is optimized to manage large traffic and transactions during peak hours to avoid technical issues that may affect customers.
    
**- Increase sales on weekdays**
  - Targeted Promotions: To increase sales, implement targeted promotions during weekday peak hours (2:00 to 4:00 p.m and 9:00 p.m). Consider offering flash deals or time-limited discounts during certain times to encourage additional purchasing.
  - Customer Engagement: Olist could use reminders or notifications to keep customers informed about offered during these hours
    
**- Enhance Payment Options**
  - Promote Credit Card Benefits: Because credit cards are the most used payment method, emphasize the advantages of utilizing them, such as installment payments or "Buy Now, Pay Later" choices. Partner with credit card providers to provide exclusive promotions or rebate offers to encourage this payment option.
  - Expand Payment Methods: Since 2020, Brazil has released new payment method call PIX from BCB and became popular. Therefore, Olist could balance the usage of these payment methods by controlling promotions from Credit Card.
    
**- Improve Logistics and Delivery Times**
  - Optimize delivery in key regions: The average time for Olist to prepare and package their orders is 2 days, because it is difficult to control the shipping problem => Decrease average time of processing orders would be a wise choice.
    
**- Increase customer loyalty**
  - Loyalty Programs: Develop and improve loyalty programs to reward repeat purchases, as loyal customers account for a large amount of income. Loyal customers might receive exclusive discounts, early access to sales, and special prizes.
  - Re-engage at-risk customers. Implement ways to re-engage at-risk clients who make substantial orders and transactions.
  - Personalized discounts, re-engagement emails, and targeted ads can all help you reclaim these consumers.


 


 
