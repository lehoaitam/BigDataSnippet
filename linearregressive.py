import os
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import linear_model
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PolynomialFeatures


df = pd.read_csv("./data/auto-mpg.csv")


# features selection
features = list(["cylinders", "displacement", "horsepower", "weight", "acceleration"])
Y = df["mpg"]
X = df[features]

# split data-set into training (70%) and testing set (30%)
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.3)

# DEFAULT MODEL
# training model
linear = linear_model.LinearRegression()
linear.fit(x_train, y_train)

# evaluating model
linear_score_trained = linear.score(x_test, y_test)
print "Model scored:", linear_score_trained

# LASSO MODEL
# L1 regularization
lasso_linear = linear_model.Lasso(alpha=1.0)
lasso_linear.fit(x_train, y_train)

# evaluating L1 regularized model
score_lasso_trained = lasso_linear.score(x_test, y_test)
print "Lasso model scored:", score_lasso_trained

# RIDGE MODEL
# L2 regularization
ridge_linear = Ridge(alpha=1.0)
ridge_linear.fit(x_train, y_train)

# evaluating L2 regularized model
score_ridge_trained = ridge_linear.score(x_test, y_test)
print "Ridge model scored:", score_ridge_trained


# POLYNOMIAL REGRESSION
poly_model = Pipeline([('poly', PolynomialFeatures(degree=2)),
                    ('linear', linear_model.LinearRegression(fit_intercept=False))])
poly_model = poly_model.fit(x_train, y_train)
score_poly_trained = poly_model.score(x_test, y_test)
print "Poly model scored:", score_poly_trained

toyota_prius = [4,497,72,3626,13.4]
toyota_prius_mpg = poly_model.predict(toyota_prius)
print "toyota_prius_mpg:", toyota_prius_mpg