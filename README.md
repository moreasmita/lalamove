# Data Engineer Assignment - Lalamove

Problem statement
As a logistics platform that matches users and drivers, we would like to know how many drivers are online, in order to determine our current supply. Online drivers will send a ping at regular intervals, which we record in the database(we can assume drivers are offline if they have not send us a ping). Our first use case is to have a streaming application, which counts the unique number of online drivers every minute.

Data:
The data used can be generated from the script, for example generated to a file would be:
python generator.py --records 1000000 > data.jsonl

The data generator can also be adjusted to test your solution, with the following options
--records number of records to generate
--seed seed for the generator
--drivers total number of drivers in the platform  

Each record contains many fields, the following fields are the useful ones:
- timestamp: Unix time of the record
- driver_id: Unique identifier of the driver
- on_duty: Whether the driver is available to take orders

Requirements:
- Stream the data into a data streaming tool of your choice (Apache Kafka, Amazon Kinesis etc.)
- Using a stream processing tool of your choice, compute the following counts for every minute
    1. How many drivers are online?
    2. Of those drivers who are online, how many of them are available to take orders? In other words, if the driver is online in that minute, how many of them were last appeared available?
- Test on correctness of the output, and if the output has errors, how much error is there?
- Measure the performance of your solution
- Documentation on the following:
    - Details on how to run the application on Linux or OS X (Docker could also be used if you prefer)
    - Your approach to tackle this problem
    - When do we output the data, and why?
    - How to handle if the application crashes?
    - How to scale this?
