import pandas as pd
import xgboost
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

features_df = pd.read_parquet('s3://dsc102-nickdegroot-scratch/2017-features.parquet')
labels_df = pd.read_parquet('s3://dsc102-nickdegroot-scratch/2017-labels.parquet')
final_df = pd.merge(features_df, labels_df, left_index=True, right_on='LoanSequenceNumber')

X = final_df.drop(['Label', 'LoanSequenceNumber'], axis=1)
y = final_df['Label']
X_tr, X_test, y_tr, y_test = train_test_split(X, y)

cf = xgboost.sklearn.XGBRegressor()
cf.fit(X_tr, y_tr)

score = r2_score(y_test, cf.classify(X_test))
print(score)