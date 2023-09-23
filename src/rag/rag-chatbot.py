# Credits to Pinecone.ai: https://github.com/pinecone-io/examples/blob/master/learn/generation/langchain/rag-chatbot.ipynb

# # Building RAG Chatbots with LangChain

# In this example, we'll work on building an AI chatbot from start-to-finish.
# We will be using LangChain, OpenAI, and Pinecone vector DB, to build a chatbot capable of learning from the external world using **R**etrieval **A**ugmented **G**eneration (RAG).
# 
# We will be using a dataset sourced from the Llama 2 ArXiv paper and other related papers to help our chatbot answer questions about the latest and greatest in the world of GenAI.
# 
# By the end of the example we'll have a functioning chatbot and RAG pipeline that can hold a conversation and provide informative responses based on a knowledge base.
# 
# ### Before you begin
# 
# You'll need to get an [OpenAI API key](https://platform.openai.com/account/api-keys) and [Pinecone API key](https://app.pinecone.io).

# ### Prerequisites

# Before we start building our chatbot, we need to install some Python libraries. Here's a brief overview of what each library does:
# 
# - **langchain**: This is a library for GenAI. We'll use it to chain together different language models and components for our chatbot.
# - **openai**: This is the official OpenAI Python client. We'll use it to interact with the OpenAI API and generate responses for our chatbot.
# - **datasets**: This library provides a vast array of datasets for machine learning. We'll use it to load our knowledge base for the chatbot.
# - **pinecone-client**: This is the official Pinecone Python client. We'll use it to interact with the Pinecone API and store our chatbot's knowledge base in a vector database.
# 
# You can install these libraries using pip like so:

# ### Building a Chatbot (no RAG)

# We will be relying heavily on the LangChain library to bring together the different components needed for our chatbot.
# To begin, we'll create a simple chatbot without any retrieval augmentation. We do this by initializing a `ChatOpenAI` object. For this we do need an [OpenAI API key](https://platform.openai.com/account/api-keys).

# %%
import os
from langchain.chat_models import ChatOpenAI

os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY") or "YOUR_API_KEY"

chat = ChatOpenAI(
    openai_api_key=os.environ["OPENAI_API_KEY"],
    model='gpt-3.5-turbo'
)

# Chats with OpenAI's `gpt-3.5-turbo` and `gpt-4` chat models are typically structured (in plain text) like this:
# 
# ```
# System: You are a helpful assistant.
# 
# User: Hi AI, how are you today?
# 
# Assistant: I'm great thank you. How can I help you?
# 
# User: I'd like to understand string theory.
# 
# Assistant:
# ```
# 
# The final `"Assistant:"` without a response is what would prompt the model to continue the conversation. In the official OpenAI `ChatCompletion` endpoint these would be passed to the model in a format like:
# 
# ```python
# [
#     {"role": "system", "content": "You are a helpful assistant."},
#     {"role": "user", "content": "Hi AI, how are you today?"},
#     {"role": "assistant", "content": "I'm great thank you. How can I help you?"}
#     {"role": "user", "content": "I'd like to understand string theory."}
# ]
# ```
# 
# In LangChain there is a slightly different format. We use three _message_ objects like so:

# %%
from langchain.schema import (
    SystemMessage,
    HumanMessage,
    AIMessage
)

messages = [
    SystemMessage(content="You are a helpful assistant."),
    HumanMessage(content="Hi AI, how are you today?"),
    AIMessage(content="I'm great thank you. How can I help you?"),
    HumanMessage(content="I'd like to understand string theory.")
]

# The format is very similar, we're just swapped the role of `"user"` for `HumanMessage`, and the role of `"assistant"` for `AIMessage`.
# 
# We generate the next response from the AI by passing these messages to the `ChatOpenAI` object.

# %%
res = chat(messages)
res

# In response we get another AI message object. We can print it more clearly like so:

# %%
print(res.content)

# Because `res` is just another `AIMessage` object, we can append it to `messages`, add another `HumanMessage`, and generate the next response in the conversation.

# %%
# add latest AI response to messages
messages.append(res)

# now create a new user prompt
prompt = HumanMessage(
    content="Why do physicists believe it can produce a 'unified theory'?"
)
# add to messages
messages.append(prompt)

# send to chat-gpt
res = chat(messages)

print(res.content)

# ### Dealing with Hallucinations

# We have our chatbot, but as mentioned — the knowledge of LLMs can be limited.
# The reason for this is that LLMs learn all they know during training. An LLM essentially compresses the "world"
# as seen in the training data into the internal parameters of the model. We call this knowledge the _parametric knowledge_ of the model.

# By default, LLMs have no access to the external world.
# 
# The result of this is very clear when we ask LLMs about more recent information, like about the new (and very popular) Llama 2 LLM.

# %%
# add latest AI response to messages
messages.append(res)

# now create a new user prompt
prompt = HumanMessage(
    content="What is so special about Llama 2?"
)
# add to messages
messages.append(prompt)

# send to OpenAI
res = chat(messages)

# %%
print(res.content)

# Our chatbot can no longer help us, it doesn't contain the information we need to answer the question.
# It was very clear from this answer that the LLM doesn't know the informaiton, but sometimes an LLM may
# respond like it _does_ know the answer — and this can be very hard to detect.
# 
# OpenAI have since adjusted the behavior for this particular example as we can see below:

# %%
# add latest AI response to messages
messages.append(res)

# now create a new user prompt
prompt = HumanMessage(
    content="Can you tell me about the LLMChain in LangChain?"
)
# add to messages
messages.append(prompt)

# send to OpenAI
res = chat(messages)

# %%
print(res.content)

# There is another way of feeding knowledge into LLMs. It is called _source knowledge_ and it refers to any
# information fed into the LLM via the prompt. We can try that with the LLMChain question.
# We can take a description of this object from the LangChain documentation.

# %%
llmchain_information = [
    "A LLMChain is the most common type of chain. It consists of a PromptTemplate, a model (either an LLM or a ChatModel), and an optional output parser. This chain takes multiple input variables, uses the PromptTemplate to format them into a prompt. It then passes that to the model. Finally, it uses the OutputParser (if provided) to parse the output of the LLM into a final format.",
    "Chains is an incredibly generic concept which returns to a sequence of modular components (or other chains) combined in a particular way to accomplish a common use case.",
    "LangChain is a framework for developing applications powered by language models. We believe that the most powerful and differentiated applications will not only call out to a language model via an api, but will also: (1) Be data-aware: connect a language model to other sources of data, (2) Be agentic: Allow a language model to interact with its environment. As such, the LangChain framework is designed with the objective in mind to enable those types of applications."
]

source_knowledge = "\n".join(llmchain_information)

# We can feed this additional knowledge into our prompt with some instructions telling the LLM how we'd like it to use this information alongside our original query.

# %%
query = "Can you tell me about the LLMChain in LangChain?"

augmented_prompt = f"""Using the contexts below, answer the query.

Contexts:
{source_knowledge}

Query: {query}"""

# Now we feed this into our chatbot as we were before.

# %%
# create a new user prompt
prompt = HumanMessage(
    content=augmented_prompt
)
# add to messages
messages.append(prompt)

# send to OpenAI
res = chat(messages)

# %%
print(res.content)

# The quality of this answer is phenomenal. This is made possible thanks to the idea of augmented our query with external knowledge (source knowledge).
# There's just one problem — how do we get this information in the first place?
# 
# We learned in the previous chapters about Pinecone and vector databases. Well, they can help us here too. But first, we'll need a dataset.

# ### Importing the Data

# In this task, we will be importing our data. We will be using the Hugging Face Datasets library to load our data.
# Specifically, we will be using the `"jamescalam/llama-2-arxiv-papers"` dataset. This dataset contains a collection of
# ArXiv papers which will serve as the external knowledge base for our chatbot.

# %%
from datasets import load_dataset

# TODO 2023-09-22: chunk our arxiv dataset
dataset = load_dataset(
    "jamescalam/llama-2-arxiv-papers-chunked",
    split="train"
)

dataset

# %%
dataset[0]

# #### Dataset Overview
# 
# The dataset we are using is sourced from the Llama 2 ArXiv papers. It is a collection of academic papers from ArXiv, a repository of electronic preprints approved for publication after moderation. Each entry in the dataset represents a "chunk" of text from these papers.
# 
# Because most **L**arge **L**anguage **M**odels (LLMs) only contain knowledge of the world as it was during training, they cannot answer our questions about Llama 2 — at least not without this data.

# ### Task 4: Building the Knowledge Base

# We now have a dataset that can serve as our chatbot knowledge base. Our next task is to transform that dataset into the knowledge base that our chatbot can use. To do this we must use an embedding model and vector database.
# 
# We begin by initializing our connection to Pinecone, this requires a [free API key](https://app.pinecone.io).

# %%
import pinecone

# get API key from app.pinecone.io and environment from console
pinecone.init(
    api_key=os.environ.get('PINECONE_API_KEY') or 'YOUR_API_KEY',
    environment=os.environ.get('PINECONE_ENVIRONMENT') or 'YOUR_ENV'
)

# Then we initialize the index. We will be using OpenAI's `text-embedding-ada-002` model for creating the embeddings, so we set the `dimension` to `1536`.

# %%
import time

index_name = 'llama-2-rag'

if index_name not in pinecone.list_indexes():
    pinecone.create_index(
        index_name,
        dimension=1536,
        metric='cosine'
    )
    # wait for index to finish initialization
    while not pinecone.describe_index(index_name).status['ready']:
        time.sleep(1)

index = pinecone.Index(index_name)

# Then we connect to the index:

# %%
index.describe_index_stats()

# Our index is now ready but it's empty. It is a vector index, so it needs vectors. As mentioned, to create these vector embeddings we will OpenAI's `text-embedding-ada-002` model — we can access it via LangChain like so:

# %%
from langchain.embeddings.openai import OpenAIEmbeddings

embed_model = OpenAIEmbeddings(model="text-embedding-ada-002")

# Using this model we can create embeddings like so:

# %%
texts = [
    'this is the first chunk of text',
    'then another second chunk of text is here'
]

res = embed_model.embed_documents(texts)
len(res), len(res[0])

# From this we get two (aligning to our two chunks of text) 1536-dimensional embeddings.
# 
# We're now ready to embed and index all our our data! We do this by looping through our dataset and embedding and inserting everything in batches.

# %%
from tqdm.auto import tqdm  # for progress bar

data = dataset.to_pandas()  # this makes it easier to iterate over the dataset

batch_size = 100

for i in tqdm(range(0, len(data), batch_size)):
    i_end = min(len(data), i+batch_size)
    # get batch of data
    batch = data.iloc[i:i_end]
    # generate unique ids for each chunk
    ids = [f"{x['doi']}-{x['chunk-id']}" for i, x in batch.iterrows()]
    # get text to embed
    texts = [x['chunk'] for _, x in batch.iterrows()]
    # embed text
    embeds = embed_model.embed_documents(texts)
    # get metadata to store in Pinecone
    metadata = [
        {'text': x['chunk'],
         'source': x['source'],
         'title': x['title']} for i, x in batch.iterrows()
    ]
    # add to Pinecone
    index.upsert(vectors=zip(ids, embeds, metadata))

# We can check that the vector index has been populated using `describe_index_stats` like before:

# %%
index.describe_index_stats()

# #### Retrieval Augmented Generation

# We've built a fully-fledged knowledge base. Now it's time to connect that knowledge base to our chatbot. To do that we'll be diving back into LangChain and reusing our template prompt from earlier.

# To use LangChain here we need to load the LangChain abstraction for a vector index, called a `vectorstore`. We pass in our vector `index` to initialize the object.

# %%
from langchain.vectorstores import Pinecone

text_field = "text"  # the metadata field that contains our text

# initialize the vector store object
vectorstore = Pinecone(
    index, embed_model.embed_query, text_field
)

# Using this `vectorstore` we can already query the index and see if we have any relevant information given our question about Llama 2.

# %%
query = "What is so special about Llama 2?"

vectorstore.similarity_search(query, k=3)

# We return a lot of text here and it's not that clear what we need or what is relevant. Fortunately, our LLM will be able to parse this information much faster than us. All we need is to connect the output from our `vectorstore` to our `chat` chatbot. To do that we can use the same logic as we used earlier.

# %%
def augment_prompt(query: str):
    # get top 3 results from knowledge base
    results = vectorstore.similarity_search(query, k=3)
    # get the text from the results
    source_knowledge = "\n".join([x.page_content for x in results])
    # feed into an augmented prompt
    augmented_prompt = f"""Using the contexts below, answer the query.

    Contexts:
    {source_knowledge}

    Query: {query}"""
    return augmented_prompt

# Using this we produce an augmented prompt:

# %%
print(augment_prompt(query))

# There is still a lot of text here, so let's pass it onto our chat model to see how it performs.

# %%
# create a new user prompt
prompt = HumanMessage(
    content=augment_prompt(query)
)
# add to messages
messages.append(prompt)

res = chat(messages)

print(res.content)

# We can continue with more Llama 2 questions. Let's try _without_ RAG first:

# %%
prompt = HumanMessage(
    content="what safety measures were used in the development of llama 2?"
)

res = chat(messages + [prompt])
print(res.content)

# The chatbot is able to respond about Llama 2 thanks to it's conversational history stored in `messages`. However, it doesn't know anything about the safety measures themselves as we have not provided it with that information via the RAG pipeline. Let's try again but with RAG.

# %%
prompt = HumanMessage(
    content=augment_prompt(
        "what safety measures were used in the development of llama 2?"
    )
)

res = chat(messages + [prompt])
print(res.content)

# We get a much more informed response that includes several items missing in the previous non-RAG response, such as "red-teaming", "iterative evaluations", and the intention of the researchers to share this research to help "improve their safety, promoting responsible development in the field".


