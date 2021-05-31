import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask_ml.preprocessing import DummyEncoder
from dask.distributed import Client
import sys


import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask_ml.preprocessing import DummyEncoder
from dask.distributed import Client
import sys


class OriginationDataFrame:
    df: dd.DataFrame

    # http://www.freddiemac.com/fmac-resources/research/pdf/user_guide.pdf
    __schema__ = {
        'Credit Score': 'Int64',
        'First Payment Date': 'object',
        'First Time Homebuyer Flag': 'category',
        'Maturity Date': 'object',
        'MSA': 'float32',
        'Mortgage Insurance Percentage': 'Int64',
        'Number of Units': 'Int64',
        'Occupancy Status': 'category',
        'CLTV': 'Int64',
        'DTI': 'Int64',
        'Original UPB': 'Int64',
        'Original LTV': 'Int64',
        'Original Interest Rate': 'float32',
        'Channel': 'category',
        'PPM Flag': 'object',
        'Amortization Type': 'category',
        'Property State': 'category',
        'Property Type': 'category',
        'Postal Code': 'Int64',
        'Loan Sequence Number': 'object',
        'Loan Purpose': 'category',
        'Original Loan Term': 'Int64',
        'Number of Borrowers': 'Int64',
        'Seller Name': 'object',
        'Servicer Name': 'object',
        'Super Conforming Flag': 'object',
        'Pre-HARP Loan Sequence Num': 'object',
        'Program': 'category',
        'HARP': 'object',
        'Property Valuation Method': 'category',
        'Interest Only': 'object',
    }

    def __init__(self, urlpath: str):
        self.df = dd.read_csv(
            urlpath,
            sep='|',
            header=None,
            names=self.__schema__.keys(),
            dtype=self.__schema__,
            date_parser=(lambda x: pd.to_datetime(x, format='%Y%m')),
            parse_dates=['First Payment Date', 'Maturity Date'],
        )
        self.df = self.df.set_index(self.df['Loan Sequence Number'])
        
        categories = self.df.select_dtypes('category')
        self.df = self.df.categorize(categories.columns)

    def process(self):
        """
        Preprocesses the origination DataFrame(s) passed through during construction then returns the result.
        """

        def set_na(series: dd.Series, *na_values):
            for val in na_values:
                series = series.replace(val, np.NaN)
            return series

        # Set null values to NaN as described in the user documentation
        features = self.df.assign(**{
            'Credit Score': set_na(self.df['Credit Score'], 9999),
            'First Time Homebuyer Flag': self.df['First Time Homebuyer Flag'] == 'Y',
            'Mortgage Insurance Percentage': set_na(self.df['Mortgage Insurance Percentage'], 0, 999),
            'Number of Units': set_na(self.df['Number of Units'], 99),
            'Occupancy Status': set_na(self.df['Occupancy Status'], 9),
            'DTI': set_na(self.df['DTI'], 999),
            'Original LTV': set_na(self.df['Original LTV'], 999),
            'Channel': set_na(self.df['Channel'], 9),
            'PPM Flag': self.df['PPM Flag'] == 'Y',
            'Purpose': set_na(self.df['Loan Purpose'], 9),
            'Number of Borrowers': set_na(self.df['Number of Borrowers'], 99),
            'Interest Only': self.df['Interest Only'] == 'Y'
        })
        # Only include certain features
        features = features[[
            'Amortization Type',
            'Channel',
            'Credit Score',
            'DTI',
            'First Time Homebuyer Flag',
            'Interest Only',
            'Mortgage Insurance Percentage',
            'Number of Borrowers',
            'Number of Units',
            'Occupancy Status',
            'Original Interest Rate',
            'Original Loan Term',
            'Original LTV',
            'Original UPB',
            'PPM Flag',
            'Property State',
            'Property Type',
            'Purpose',
        ]]

        # One-hot-encode categorical variables
        processed = DummyEncoder().fit_transform(features)
        return processed


if __name__ == '__main__':
    client = Client()

    origination_path = sys.argv[1]
    result_path = sys.argv[2]

    df = OriginationDataFrame(origination_path).process()
    df.to_parquet(result_path)

    client.close()
