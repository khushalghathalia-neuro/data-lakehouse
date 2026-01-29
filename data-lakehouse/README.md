# Data Lakehouse – Synthetic NYC Taxi Dataset

## Overview
This project contains a **synthetic NYC Taxi dataset** designed for **data analysis and Apache Spark / PySpark practice**.  
The data mimics real NYC taxi trip records but **does not contain any real or sensitive information**.

## DataSet 1 contain general data of taxi and customer.

Column Name	   Data Type	Description
vendor_id	   Integer	Taxi vendor identifier
pickup_datetime	   Timestamp	Trip pickup date & time
dropoff_datetime   Timestamp	Trip drop-off date & time
passenger_count	   Integer	Number of passengers
trip_distance	   Double	Distance traveled (miles)
pickup_longitude   Double	Pickup longitude
pickup_latitude	   Double	Pickup latitude
dropoff_longitude  Double	Drop-off longitude
dropoff_latitude   Double	Drop-off latitude
fare_amount	   Double	Base fare amount
tip_amount	   Double	Tip amount
total_amount	   Double	Total charged amount
payment_type	   Integer	Payment method code

## DataSet 2 contain payment information/ways

Column Name           Data Type   Description
payment_type          Double      Type of payment
payment_description   Double      Description of payment
payment_method        Double      Mode of Payment
transaction_fee_pct   Double      Fees on Transaction

## Partition
Data is partition on basis of pickup_datetime on day basis.