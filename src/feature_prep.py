import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask_ml.preprocessing import DummyEncoder
from dask.distributed import Client
from os import path
import sys


class OriginationDataFrame:
    df: dd.DataFrame
    # http://www.freddiemac.com/fmac-resources/research/pdf/user_guide.pdf
    col_names = [
        'Credit Score',
        'First Payment Date',
        'First Time Homebuyer',
        'Maturity Date',
        'MSA',
        'Mortgage Insurance Percentage',
        'Num Units',
        'Occupancy Status',
        'CLTV',
        'DTI',
        'Original UPB',
        'Original LTV',
        'Original Interest Rate',
        'Channel',
        'PPM',
        'Amortization',
        'Property State',
        'Property Type',
        'Postal Code',
        'Loan Seq Num',
        'Loan Purpose',
        'Original Loan Term',
        'Num Borrowers',
        'Seller Name',
        'Servicer Name',
        'Super Conforming',
        'Pre-HARP Loan Seq Num',
        'Program',
        'HARP',
        'Property Valuation Method',
        'Interest Only',
    ]

    def __init__(self, urlpath: str):
        self.df = dd.read_csv(
            urlpath,
            sep='|',
            header=None,
            names=self.col_names,
            date_parser=(lambda x: pd.to_datetime(x, format='%Y%m')),
            parse_dates=['First Payment Date', 'Maturity Date'],
            true_values=['Y'],
            false_values=['N'],
            dtype={
                'Credit Score': 'uint16',
                # 'First Payment Date': 'datetime64',
                'First Time Homebuyer': 'category',
                # 'Maturity Date': 'datetime64',
                'MSA': 'float32',
                'Mortgage Insurance Percentage': 'uint16',
                'Num Units': 'uint8',
                'Occupancy Status': 'category',
                'CLTV': 'uint16',
                'DTI': 'uint16',
                'Original UPB': 'uint32',
                'Original LTV': 'uint16',
                'Original Interest Rate': 'float32',
                'Channel': 'category',
                'PPM': 'bool',
                'Amortization': 'category',
                'Property State': 'category',
                'Property Type': 'category',
                'Postal Code': 'uint16',
                'Loan Seq Num': 'object',
                'Loan Purpose': 'category',
                'Original Loan Term': 'uint16',
                'Num Borrowers': 'uint8',
                'Seller Name': 'object',
                'Servicer Name': 'object',
                'Super Conforming': 'object',
                'Pre-HARP Loan Seq Num': 'object',
                'Program': 'category',
                'HARP': 'object',
                'Property Valuation Method': 'category',
                'Interest Only': 'bool',
            }
        )
        self.df = self.df.set_index(self.df['Loan Seq Num'])
        
        categories = self.df.select_dtypes('category')
        self.df = self.df.categorize(categories.columns)

    def process(self):
        """
        Preprocesses the origination DataFrame(s) passed through during construction then returns the result.
        """

        desired_cols = set([
            'Credit Score',
            'First Time Homebuyer',
            'Mortgage Insurance Percentage',
            'Num Units',
            'CLTV',
            'DTI',
            'Original LTV',
            'Original Interest Rate',
            'Amortization',
            'Property Type',
            'Loan Purpose',
            'Original Loan Term',
            'Multi Borrowers',
            'Primary Home',
        ])

        nan_col = (
            (self.df['Credit Score'] == 9999)
            | (self.df['Num Borrowers'] == 99)
            | (self.df['First Time Homebuyer'] == '9')
            | (self.df['Occupancy Status'] == '9')
        )

        features = (
            self.df
            .assign(
                **{
                    'First Time Homebuyer': self.df['First Time Homebuyer'] == 'Y',
                    'Num Units': self.df['Num Units'].replace({ 99: np.NaN }),
                    'Multi Borrowers': self.df['Num Borrowers'] == 2,
                    'Primary Home': self.df['Occupancy Status'] == 'P',
                }
            )
            .drop(columns=set(self.col_names).difference(desired_cols))
            [~nan_col]
        )

        processed = DummyEncoder().fit_transform(features)
        return processed


if __name__ == '__main__':
    client = Client()

    origination_path = sys.argv[1]
    result_path = sys.argv[2]

    df = OriginationDataFrame(origination_path).process()
    df.to_parquet(result_path)

    client.close()
