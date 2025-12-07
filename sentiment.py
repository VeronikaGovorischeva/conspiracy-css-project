import pandas as pd
from nrclex import NRCLex
from tqdm import tqdm

df = pd.read_csv("dataset.csv", low_memory=False)

df["text"] = df["body"].fillna("") + " " + df["title"].fillna("")

emotion_cols = [
    "anger", "anticipation", "disgust", "fear",
    "joy", "sadness", "surprise", "trust"
]

for col in emotion_cols:
    df[col] = 0

tqdm.pandas(desc="Analyzing sentiment")

def extract_emotions(text):
    if not isinstance(text, str) or text.strip() == "":
        return pd.Series([0]*8, index=emotion_cols)

    emo = NRCLex(text)
    raw = emo.raw_emotion_scores

    return pd.Series([raw.get(e, 0) for e in emotion_cols], index=emotion_cols)

df[emotion_cols] = df["text"].progress_apply(extract_emotions)

df["length"] = df["text"].str.len()

df.to_csv("sentiment.csv", index=False)

print("Sentiment analysis completed → sentiment.csv")
