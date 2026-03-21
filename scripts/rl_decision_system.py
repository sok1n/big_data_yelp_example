import gymnasium as gym
import numpy as np
import random
import joblib
import pandas as pd

# load the trained model
model = joblib.load("models/high_rating_model.pkl")

# keep rows with required fields

df = pd.read_csv("output/yelp_academic_dataset_business_flat.csv")

# keep rows with required fields
df = df.dropna(subset=["review_count", "is_open"])

#create RL environment

env = gym.make("Taxi-v3")

# create Q-table
Q = np.zeros(
    (
        env.observation_space.n,
        env.action_space.n
    )
)

# hyperparameters
alpha = 0.1 # learning rate
gamma = 0.6 # discount factor
epsilon = 0.1 # exploration rate

# training loop

for epsilon in range (500) :

    state, _ = env.reset()
    done = False

    while not done:
        # sample one business from the dataset
        sample = df.sample(1)

        # extract features as in ML model
        features = sample[
            [
                "review_count",
                "is_open"
            ]
        ]

        # predict if the business is likely high-rated
        prediction = model.predict(features)[0]

        # give a reward bonus based on ML prediction
        if prediction == 1:
            reward_bonus = 5
        else:
            reward_bonus = -2

        # epsilon-greedy action choice
        if random.random() < epsilon:
            action = env.action_space.sample()
        else:
            action = np.argmax(Q[state])
        
        # take action in environment
        next_state, reward, terminated, truncated, _ = env.step(action)

        #combine environment reward with ML-based reward bonus
        total_reward = reward + reward_bonus

        #Q-learning
        Q[state, action] = Q[state, action] + alpha *(
           total_reward + gamma * np.max(Q[next_state]) - Q[state, action])
        
        state = next_state
        done = terminated or truncated