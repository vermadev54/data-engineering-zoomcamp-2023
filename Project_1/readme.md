
## Index
- [Problem Description](#problem-description)
- [Dataset](#dataset)
- [Technologies Used](#technologies-used)
- [Steps for Project Reproduction](#steps-for-project-reproduction)
- [Summary of DAG of Data Pipeline and decisions regarding transformations](#Summary-of-DAG-of-Data-Pipeline-and-decisions-regarding-transformations)
- [Dashboard](#dashboard)
- [Conclusion](#conclusion)

# Problem Description
A digital platform known as a crypto dashboard resides on a website or an app (either desktop or mobile). Its main job is to keep track of your cryptocurrency coins, keep track of their historical prices, and keep an eye on their current values so you can manage your crypto assets and associated financial plans accordingly. To update your crypto assets in real-time, the dashboard integrates with cryptocurrency exchanges or trackers like CoinCap API via an API or other tool.

- Real-time crypto dashboards for cryptocurrency data can provide several benefits, including:

- Up-to-date information: Real-time dashboards allow you to access the latest data on cryptocurrency prices, trading volume, market capitalization, and other important metrics. This information can help you make more informed decisions about buying, selling, or holding cryptocurrencies.

- Quick insights: With a real-time dashboard, you can quickly analyze large amounts of data and identify trends or patterns that may not be immediately apparent. This can help you stay ahead of the curve and make more informed trading decisions.

- Improved accuracy: Real-time dashboards provide accurate, up-to-date information that can help you avoid costly mistakes. With accurate data at your fingertips, you can make more informed decisions about trading or investing in cryptocurrencies.

- Customization: Real-time dashboards can be customized to suit your needs, allowing you to focus on the data that is most important to you. You can choose which data to display, how it is presented, and how often it is updated.

Overall, real-time dashboards can help you stay informed, make better decisions, and stay ahead of the curve when it comes to trading or investing in cryptocurrencies.


# Dataset

 - [Coinbase](https://docs.pro.coinbase.com/#websocket-feed) - Coinbase provides a free market data fee; giving access to real-time price updates, level 2 data, orders and matches. This is a high frequency feed with hundreds of events each second.





# Technologies Used

For this project I decided to use the following tools:
- **Infrastructure as code (IaC):** Terraform
- **Workflow orchestration:** Airflow
- **Containerization:** Docker
- **Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** BigQuery
- **Transformations:** SQL (Pyspark will be used in the future with more time and one less technical error!) 
- **Visualization:** Google Data Studio

# Steps for Project Reproduction
