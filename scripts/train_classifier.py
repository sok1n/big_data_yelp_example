import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import joblib

# load the dataset = flattened businees data
df = df = pd.read_csv('output/yelp_academic_dataset_business_flat.csv')

# keep only rows for the classifier
# we are going to predict the "high _rating" based on "stars", "review_count", "is open"
df.dropna(subset=['stars', 'review_count', 'is_open']) # features for the model

#create target variable
#1=high rating, 0=not high rating
df['high_rating'] = df['stars'].apply(
    lambda x: 1 if x >= 4 else 0
)

#matrxis with the features of the model
X = df[
    [
        "review_count",
        "is_open"
    ]
]

#define the target,vector
y = df['high_rating']

#split tarin and test
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
)

#create the classifier
model = RandomForestClassifier()
model.fit(X_train, y_train)

predictions = model.predict(X_test)

#evaluate the classifier
accuracy = accuracy_score(y_test, predictions)
print("Model accuracy:", accuracy)

#save the trained model
joblib.dump(model, "models/high_rating_model.pkl")
print("Model saved")
