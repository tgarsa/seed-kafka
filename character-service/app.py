
from fastapi import FastAPI
from pydantic import BaseModel
##
import nltk
# from nltk.tokenize import word_tokenize
nltk.download('punkt')

characters = set()
with open("characters.txt", 'r') as file:
    for line in file:
        characters.add(line.replace("\n", ""))

class Input(BaseModel):
    text: str

app = FastAPI(title="Classification pipeline",
              description="A simple text classifier",
              version="1.0")

@app.post('/characters', tags=["predictions"])
async def get_prediction(incoming_data: Input):
    words_in_text = nltk.tokenize.word_tokenize(incoming_data.text)
    filtered_list = set()
    for word in words_in_text:
        if word in characters:
            filtered_list.add(word)

    return {"characters": filtered_list}
