import pandas as pd
import numpy as np
import os

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from env import host, user, password


def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    It takes in a string name of a database as an argument.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

    
# One function to do all
def mall_final():
    '''
    This function reads the mall customer data from the Codeup db into a df and returns the df.
    '''
    # Create SQL query.
    sql_query_mall = """
    select * from customers;
    """
    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query_mall, get_connection('mall_customers'))
    
    # remove outliers 
    df = remove_outliers(df, k, ['age', 'spending_score', 'annual_income'])
    
    # Encoding the gender column
    dummy_df = pd.get_dummies(df.gender, drop_first = True)

    # concat the dummies df with original df.
    df = pd.concat ([df, dummy_df], axis = 1)
    
    # splitting data into data set
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_validate, test_size=.3, random_state=123)
    
    return train, validate, test
    


def new_mall_customers():
    '''
    This function reads the mall customer data from the Codeup db into a df and returns the df.
    '''
    # Create SQL query.
    sql_query_mall = """
    select * from customers;
    """
    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query_mall, get_connection('mall_customers'))
    
    # Write DataFrame to a csv file.
    df.to_csv('mallcustomers.csv')
    
    
    return df


def remove_outliers(df, k , col_list):
    ''' remove outliers from a list of columns in a dataframe 
        and return that dataframe
    '''
    
    for col in col_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles
        
        iqr = q3 - q1   # calculate interquartile range
        
        upper_bound = q3 + k * iqr   # get upper bound
        lower_bound = q1 - k * iqr   # get lower bound

        # return dataframe without outliers
        
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        
    return df


def split_mall(df):
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_validate, test_size=.3, random_state=123)
    return train, validate, test


def min_max_scaler1(train, validate, test):
    # MinMax Scaling
    scaler = sklearn.preprocessing.MinMaxScaler()
    # Note that we only call .fit with the training data,
    # but we use .transform to apply the scaling to all the data splits.
    scaler.fit(train)

    train_scaled = scaler.transform(train)
    validate_scaled = scaler.transform(validate)
    test_scaled = scaler.transform(test)
    return scaler, train, validate, test

# One function to do all
def mall_final():
    '''
    This function reads the mall customer data from the Codeup db into a df and returns the df.
    '''
    # Create SQL query.
    sql_query_mall = """
    select * from customers;
    """
    
    # Read in DataFrame from Codeup db.
    df = pd.read_sql(sql_query_mall, get_connection('mall_customers'))
    
    # remove outliers 
    df = remove_outliers(df, 1.5, ['age', 'spending_score', 'annual_income'])
    
    # Encoding the gender column
    dummy_df = pd.get_dummies(df.gender, drop_first = True)

    # concat the dummies df with original df.
    df = pd.concat ([df, dummy_df], axis = 1)
    
    # splitting data into data set
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_validate, test_size=.3, random_state=123)
    
    return train, validate, test

