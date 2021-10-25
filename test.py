# just get the launch.json so I can debug python and step into libraries

from sentence_transformers import SentenceTransformer
sentences = ["This is an example sentence", "Each sentence is converted"]

model = SentenceTransformer('sentence-transformers/average_word_embeddings_glove.840B.300d')
embeddings = model.encode(sentences)
print(embeddings)
