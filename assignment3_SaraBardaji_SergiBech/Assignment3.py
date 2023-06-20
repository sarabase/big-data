from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

from sklearn.utils import shuffle
from sklearn.linear_model import LogisticRegression
import pickle

# Define the local path for storing files
local_path = Variable.get('local_path')

def download_and_concatenate_data():
    """
    Downloads and concatenates multiple CSV files into a single DataFrame.
    
    Inputs:
        - None
        
    Returns:
        - None
    """
    
    # Initialize an empty DataFrame
    train_df = pd.DataFrame()

    # Get the path to download the CSV files
    path_train = Variable.get('download_csv')
    
    # Iterate over each file in the downloaded CSV files
    for file in requests.get(path_train).text.split("\n"):
        # Read the CSV file and concatenate it to the train_df DataFrame
        if file:
            train_df = pd.concat([train_df, pd.read_csv(file)])

    # Save the concatenated DataFrame to a CSV file named 'train.csv'
    train_df.to_csv(os.path.join(local_path, 'train.csv'))

    # Print the shape of the DataFrame
    print("Train DataFrame shape:", train_df.shape)

def read_and_train():
    """
    Reads the 'train.csv' file, performs training on the data, and saves the trained model.

    Inputs:
        - None

    Returns:
        - None
    """

    # Read the 'train.csv' file into a DataFrame
    train_df = pd.read_csv(os.path.join(local_path, 'train.csv'))

    # Extract the features (X) and the target variable (y)
    X = train_df.drop(['Species'], axis=1).values
    y = train_df['Species']

    # Shuffle the data
    X_new, y_new = shuffle(X, y, random_state=0)

    # Split the data into training and testing sets
    n_samples_train = 120  # Number of samples for training
    X_train = X_new[:n_samples_train, :]
    y_train = y_new[:n_samples_train]

    # Initialize and train a logistic regression classifier
    clf = LogisticRegression()
    clf.fit(X_train, y_train)

    # Save the trained model to a file named 'trained_model.pkl'
    with open(os.path.join(local_path, 'trained_model.pkl'), 'wb') as f:
        pickle.dump(clf, f)

def download_test():
    """
    Downloads a test CSV file and saves it as 'test.csv'.

    Inputs:
        - None

    Returns:
        - None
    """

    # Get the URL of the test CSV file
    test_file_url = Variable.get('download_csv_test')

    # Read the test CSV file into a DataFrame
    test_df = pd.read_csv(test_file_url)

    # Save the test DataFrame to a CSV file named 'test.csv'
    test_df.to_csv(os.path.join(local_path, 'test.csv'))

def predict():
    """
    Loads a trained model, makes predictions on test data, and saves the predictions.

    Inputs:
        - None

    Returns:
        - None
    """
    
    # Load the trained model from 'trained_model.pkl'
    with open(os.path.join(local_path, 'trained_model.pkl'), 'rb') as f:
        clf_loaded = pickle.load(f)

    # Read the test data from 'test.csv' into a DataFrame
    X_test = pd.read_csv(os.path.join(local_path, 'test.csv'))

    # Make predictions on the test data using the loaded model
    y_pred = clf_loaded.predict(X_test)

    # Create a DataFrame with the predicted values
    pred_df = pd.DataFrame(y_pred)

    # Save the predictions to a CSV file named 'test_predictions.csv'
    pred_df.to_csv(os.path.join(local_path, 'test_predictions.csv'))

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'Sara',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Assignment3',
    default_args=default_args,
    description="Assignment3",
    schedule_interval="0 20 * * 1",  # Cron expression for 8 PM every Monday
)

#    0 represents the minute field and specifies that the task should be scheduled at the 0th minute of the hour.
#    20 represents the hour field and specifies that the task should be scheduled at the 20th hour of the day (8 PM).
#    * represents the day of the month field and specifies that the task should run every day of the month.
#    * represents the month field and specifies that the task should run every month.
#    1 represents the day of the week field and specifies that the task should run on Monday.

# Define the task
task1 = PythonOperator(
    task_id="download_and_concatenate_task",
    python_callable=download_and_concatenate_data,
    dag=dag
)

task2 = PythonOperator(
    task_id="read_and_train_task",
    python_callable=read_and_train,
    dag=dag
)

task3 = PythonOperator(
    task_id="download_test_task",
    python_callable=download_test,
    dag=dag
)

task4 = PythonOperator(
    task_id="predict_task",
    python_callable=predict,
    dag=dag
)

task1 >> task2 >> task4
task3 >> task4



