import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

# ================== input values ==================

repair_cost = 2000
closing_cost_rate = 3.5
tax_rate = 2.2
loan_rate = 4.25
load_duration = 30
water_fee = 100
insurance_fee = 100
vacancy_expense = 5
repair_expense = 5
capex_expense = 5
management_expense = 8

# ================== calculate function ==================

def rental_analyze(df, repair_cost, closing_cost_rate,
    tax_rate, loan_rate, load_duration, water_fee,
    insurance_fee, vacancy_expense, repair_expense,
    capex_expense, management_expense):

    # estimate rent from zestimate
    df['rent estimate'] = df['api rent zestimate']
    for i in range(df.shape[0]):
        if pd.isnull(df.loc[i, 'rent estimate']):
            df.loc[i, 'rent estimate'] = df.loc[i, 'api zestimate'] * 0.008207

    df['closing cost'] = df['api zestimate'] * closing_cost_rate / 100
    df['prop tax'] = df['api zestimate'] * tax_rate / 100
    df['total project cost'] = df['api zestimate'] + df['closing cost'] + repair_cost

    df['down payment'] = df['api zestimate'] * 0.25
    df['loan amount'] = df['api zestimate'] - df['down payment']

    r = loan_rate/100/12
    n = load_duration * 12
    df['monthly p&i'] = df['loan amount']*(r*(1+r)**n)/((1+r)**n-1)
    df['total cash needed'] = df['down payment'] + df['closing cost']

    df['vacancy expense'] = df['rent estimate'] * vacancy_expense / 100
    df['repair expense'] = df['rent estimate'] * repair_expense / 100
    df['capex expense'] = df['rent estimate'] * capex_expense / 100
    df['management expense'] = df['rent estimate'] * management_expense / 100

    df['hoa expense'] = float('Nan')
    # correcting hoa fee: if type == 'Single Family' and hoa > 200 --> div by 12
    # if no HOA data, assume it's 60 (to be conservative)
    for i in range(df.shape[0]):
        hoa_expense = 60
        if isfloat(df.loc[i, 'hoa']):
            hoa_expense = float(df.loc[i, 'hoa'])
            if df.loc[i, 'type'] == 'Single Family' and hoa_expense > 200:
                hoa_expense = hoa_expense / 12
        df.loc[i, 'hoa expense'] = hoa_expense

    df['monthly expenses'] = df['vacancy expense'] + df['repair expense'] + df['capex expense'] + df['hoa expense'] + df['management expense'] + (df['prop tax']/12) + water_fee + insurance_fee + df['monthly p&i']
    df['monthly cashflow'] = df['rent estimate'] - df['monthly expenses']
    df['cash on cash roi'] = df['monthly cashflow'] * 12 / df['total cash needed'] * 100

    return df

# ================== main code ==================

if __name__ == "__main__":
    df = pd.read_csv('data.csv')
    rental_analyze(df, repair_cost, closing_cost_rate,
        tax_rate, loan_rate, load_duration, water_fee,
        insurance_fee, vacancy_expense, repair_expense,
        capex_expense, management_expense).to_csv('data_analyzed.csv', index=False)

#TODO: impute missing hoa, api zestimate