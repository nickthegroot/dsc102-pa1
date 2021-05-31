from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructType
import sys

class PerformanceDataFrame:
    df: DataFrame
    spark: SparkSession

    _schema = (StructType()
        .add('Loan Sequence Number', StringType(), True)
        .add('Monthly Reporting Period', DateType(), True)
        .add('Current Actual UPB', FloatType(), True)
        .add('Current Loan Delinquency Status', StringType(), True)
        .add('Loan Age', IntegerType(), True)
        .add('Remaining Months to Legal Maturity', IntegerType(), True)
        .add('Repurchase Flag', StringType(), True)
        .add('Modification Flag', StringType(), True)
        .add('Zero Balance Code', IntegerType(), True)
        .add('Zero Balance Effective Date', DateType(), True)
        .add('Current Interest Rate', FloatType(), True)
        .add('Current Deferred UPB', IntegerType(), True)
        .add('DDLPI', DateType(), True)
        .add('MI Recoveries', FloatType(), True)
        .add('Net Sales Proceeds', StringType(), True)
        .add('Non MI Recoveries', FloatType(), True)
        .add('Expenses', FloatType(), True)
        .add('Legal Costs', FloatType(), True)
        .add('Maintenance Costs', FloatType(), True)
        .add('Taxes and Insurance', FloatType(), True)
        .add('Miscellaneous Expenses', FloatType(), True)
        .add('Actual Loss Calculation', FloatType(), True)
        .add('Modification Cost', FloatType(), True)
        .add('Step Modification Flag', StringType(), True)
        .add('Deferred Payment Plan', StringType(), True)
        .add('ELTV', FloatType(), True)
        .add('Zero Balance Removal UPB', FloatType(), True)
        .add('Delinquent Accrued Interest', FloatType(), True)
        .add('Delinquency Due to Disaster', StringType(), True)
        .add('Borrower Assistance Status Code', StringType(), True)
    )

    def __init__(self, urlpath: str):
        self.df = (
            spark.read
            .option("header", "false")
            .option("delimiter", "|")
            .schema(self._schema)
            .csv(urlpath)
        )


    def process(self):

        def process_row(row):
            """
            Your target label is defined as default at 90 days or more delinquent. This is reflected by “Current Loan Delinquency Status’ in the monthly performance data.
            In addition to delinquency status, if you find ‘zero balance code’ in the performance data listed as ’03’, ’06’, or ’09’, the loan should also be considered a default.
            Default should be encoded as an integer 1 if the default conditions hold, otherwise 0.
            """

            label: int
            if row['Current Loan Delinquency Status'] not in { 'XX', '0', '1', '2', ' ' }:
                label = 1
            elif row['Zero Balance Code'] in { 3, 6, 9 }:
                label = 1
            else:
                label = 0

            return (row['Loan Sequence Number'], label)

        loan_updates = self.df.rdd.map(process_row).toDF(["LoanSequenceNumber", "Label"])
        return loan_updates.groupBy('LoanSequenceNumber').max('Label').withColumnRenamed('max(Label)', 'Label')


if __name__ == '__main__':
    # Operation: spark- label_prep.py 
    spark = SparkSession.builder.getOrCreate()
    data_src = sys.argv[1]
    output_src = sys.argv[2]

    df = PerformanceDataFrame(data_src)
    processed = df.process()
    processed.write.parquet(output_src)