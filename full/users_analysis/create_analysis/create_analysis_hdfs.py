from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import time
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
import torch
import torch.nn as nn
import torch.optim as optim
from torch.autograd import Variable

schema = StructType([
    StructField("day", StringType(), True),
    StructField("predicted_count", IntegerType(), True)
])

def path_exists(spark, path):
    try:
        return len(spark.read.parquet(path).take(1)) > 0
    except AnalysisException:
        return False

def create_analysis_month():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisCreateTime").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        input_hdfs_path = "hdfs://master:9000/output/create_analysis"
        output_hdfs_path = "hdfs://master:9000/output/create_analysis_result"

        if not path_exists(spark, input_hdfs_path):
            continue

        input_data = spark.read.parquet(input_hdfs_path)

        year_month_counts = defaultdict(int)

        earliest_date = input_data.agg(F.min(F.col("created_at").cast("timestamp"))).collect()[0][0]
        latest_date = input_data.agg(F.max(F.col("created_at").cast("timestamp"))).collect()[0][0]

        for entry in input_data.collect():
            created_at = entry.get("created_at")
            if created_at:
                year_month = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m")
                year_month_counts[year_month] += 1

        all_year_months = [earliest_date + relativedelta(months=n) 
                        for n in range((latest_date.year - earliest_date.year) * 12 + (latest_date.month - earliest_date.month + 1))]
        all_year_months = [date.strftime("%Y-%m") for date in all_year_months]

        result_data = [{"year_month": date, "count": year_month_counts.get(date, 0)} for date in all_year_months]

        result_data_df = spark.createDataFrame(result_data)
        result_data_df.write.mode("overwrite").json(output_hdfs_path)

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

def create_analysis_day():
    while True:
        spark = SparkSession.builder.appName("GitHubAnalysisCreateTime").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0").getOrCreate()

        input_hdfs_path = "hdfs://master:9000/output/create_analysis"
        output_hdfs_path = "hdfs://master:9000/output/create_analysis_result"

        if not path_exists(spark, input_hdfs_path):
            continue

        input_data = spark.read.parquet(input_hdfs_path)

        day_counts = defaultdict(int)
        earliest_date = input_data.agg(F.min(F.col("created_at").cast("timestamp"))).collect()[0][0]
        latest_date = input_data.agg(F.max(F.col("created_at").cast("timestamp"))).collect()[0][0]

        for entry in input_data.collect():
            created_at = entry['created_at']
            if created_at:
                day = created_at.strftime("%Y-%m-%d")
                day_counts[day] += 1

        all_days = [earliest_date + timedelta(days=n) for n in range((latest_date - earliest_date).days + 1)]
        all_days = [date.strftime("%Y-%m-%d") for date in all_days]

        result_data = [{"day": date, "count": day_counts.get(date, 0)} for date in all_days]

        result_data_df = spark.createDataFrame(result_data)
        result_data_df.write.mode("overwrite").json(output_hdfs_path)

        predictions_df = predict_future_values(result_data)
        # data = [(datetime.strptime(row['day'], '%Y-%m-%d'), int(row['predicted_count'])) for row in predictions_df.to_dict(orient='records')]
        data = [(row['day'], int(row['predicted_count'])) for row in predictions_df.to_dict(orient='records')]
        rdd = spark.sparkContext.parallelize(data)
        predictions_df = spark.createDataFrame(rdd, schema=schema)

        # predictions_df.to_json(output_hdfs_path + "/predictions", orient="records", lines=True)

        es_host = <your-es-ip>
        es_port = <your-es-port>
        es_index1 = "users_create_time_result"
        es_index2 = "users_predict_create_time_result"

        result_data_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index1) \
                    .option("es.mapping.id", "day") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/create_analysis_result") \
                    .mode("overwrite") \
                    .save()
        
        predictions_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", es_host)\
                    .option("es.port", es_port) \
                    .option("es.resource", es_index2) \
                    .option("es.mapping.id", "day") \
                    .option("es.write.operation", "upsert") \
                    .option("es.index.auto.create", "true") \
                    .option("checkpointLocation", "hdfs://master:9000/checkpoint/elastic/predict_create_analysis_result") \
                    .mode("overwrite") \
                    .save()

        spark.stop()

        print("wait for an hour.")
        time.sleep(3600)

def predict_future_values(data):
    if len(data) > 180:
        train_data = data[-180:]
        model, scaler = train_lstm_model(train_data)
        predictions = predict_lstm(train_data, model, 10, scaler)
    elif 10 < len(data) <= 180:
        train_data = data[-10:]
        predictions = linear_regression_predict(train_data, 10)
    else:
        predictions = []

    return predictions

# def linear_regression_predict(data):
#     df = pd.DataFrame(data[-20:])
#     df['day'] = pd.to_datetime(df['day'])
#     df = df.set_index('day')

#     df['day_index'] = np.arange(len(df))

#     train = df.iloc[:-10]
#     test = df.iloc[-10:]

#     model = LinearRegression()
#     model.fit(train[['day_index']], train['count'])

#     future_days = np.arange(len(df), len(df) + 10).reshape(-1, 1)
#     predictions = model.predict(future_days)

#     future_dates = [df.index[-1] + timedelta(days=i) for i in range(1, 11)]

#     future_dates_str = [pd.to_datetime(timestamp, unit='ms') \
#                         .strftime('%Y-%m-%d') for timestamp in future_dates]

#     predicted_data = pd.DataFrame({'day': future_dates_str, 'predicted_count': predictions.astype(int)})

#     predicted_data.to_json("predict_result.json", orient='records', lines=True)

#     return predicted_data

def linear_regression_predict(train_data, n_steps):
    count_values = [item['count'] for item in train_data]

    model = LinearRegression()
    model.fit(np.arange(len(count_values)).reshape(-1, 1), count_values)

    future_days = np.arange(len(count_values), len(count_values) + n_steps).reshape(-1, 1)
    predictions = model.predict(future_days)

    future_dates = [pd.to_datetime(train_data[-1]['day'], format='%Y-%m-%d') + timedelta(days=i) for i in range(1, n_steps + 1)]
    future_dates_str = [date.strftime('%Y-%m-%d') for date in future_dates]

    predictions_df = pd.DataFrame({'day': future_dates_str, 'predicted_count': predictions.astype(int)})

    return predictions_df

class LSTM(nn.Module):
    def __init__(self, input_size=1, hidden_layer_size=100, output_size=1):
        super().__init__()
        self.hidden_layer_size = hidden_layer_size
        self.lstm = nn.LSTM(input_size, hidden_layer_size)
        self.linear = nn.Linear(hidden_layer_size, output_size)
        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                            torch.zeros(1, 1, self.hidden_layer_size))

    def forward(self, input_seq):
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out.view(len(input_seq), -1))
        return predictions[-1]


def train_lstm_model(train_data):
    count_values = [item['count'] for item in train_data]

    scaler = MinMaxScaler()
    train_data_normalized = scaler.fit_transform(np.array(count_values).reshape(-1, 1))

    X, y = [], []
    for i in range(len(train_data_normalized) - 20):
        X.append(train_data_normalized[i:i + 20])
        y.append(train_data_normalized[i + 20])

    X, y = np.array(X), np.array(y)

    X_train = torch.from_numpy(X).float()
    y_train = torch.from_numpy(y).float()

    model = LSTM()
    loss_function = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.005)

    for epoch in range(100):
        for i in range(len(X_train)):
            optimizer.zero_grad()
            model.hidden_cell = (torch.zeros(1, 1, model.hidden_layer_size),
                                torch.zeros(1, 1, model.hidden_layer_size))

            y_pred = model(X_train[i])
            single_loss = loss_function(y_pred, y_train[i])
            
            single_loss.backward()
            optimizer.step()

    return model, scaler

def predict_lstm(train_data, model, n_steps, scaler):
    count_values = [item['count'] for item in train_data]

    future_values = []
    last_sequence = torch.from_numpy(np.array(count_values[-20:])).float().view(-1, 1)
    
    for _ in range(n_steps):
        model.hidden = (torch.zeros(1, 1, model.hidden_layer_size),
                        torch.zeros(1, 1, model.hidden_layer_size))
        prediction = model(last_sequence)
        future_values.append(prediction.item())
        last_sequence = torch.cat((last_sequence[1:], prediction.view(-1, 1)))

    future_values = np.array(future_values).reshape(-1, 1)
    future_values = scaler.inverse_transform(future_values)

    future_dates = [pd.to_datetime(train_data[-1]['day'], format='%Y-%m-%d') + timedelta(days=i) for i in range(1, n_steps + 1)]
    future_dates_str = [date.strftime('%Y-%m-%d') for date in future_dates]

    predictions_df = pd.DataFrame({'day': future_dates_str, 'predicted_count': future_values.flatten().astype(int)})

    return predictions_df

if __name__ == "__main__":
    create_analysis_day()
