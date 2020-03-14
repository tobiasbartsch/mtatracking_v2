import sys
sys.path.append('/home/tbartsch/source/repos')
from mtatracking_v2.get_feature_matrix import get_current_features_NOW
import pandas as pd
import dill
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge, LogisticRegression
from sklearn.model_selection import GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.tree import DecisionTreeRegressor
from sklearn.decomposition import PCA


def makePrediction(session):
    rcols = pd.read_csv('features.csv')['0']
    t = get_current_features_NOW(
        '237N', '225N', '2', 'N',
        ['1', '2', '3', '4', '5', '6'], ['N'], 8, 'Weekday', session)
    with open("2line_weekday_estimator_withMTAPred.dill", "rb") as dill_file:
        gs = dill.load(dill_file)
    for col in (set(rcols) - set(t.columns)):
        t[col] = 0
    t = t[rcols]
    my_prediction = gs.predict(t)/60
    mta_prediction = t['mta_prediction']/60

    return int(my_prediction), int(mta_prediction)
