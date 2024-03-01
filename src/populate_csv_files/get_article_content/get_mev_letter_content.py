
from llama_index.llms import OpenAI, HuggingFaceLLM, ChatMessage, MessageRole
OPENAI_MODEL_NAME = "gpt-3.5-turbo"
prompt = """Given this content, extract the links of each source, their author, and when relevant the reference to that source like a Twitter thread.
            Return that in the comma separated format with a newline for each unique source to match .csv requirements for instance"""

def get_inference_llm(llm_model_name):
    if llm_model_name in OPENAI_INFERENCE_MODELS:
        return OpenAI(model=llm_model_name)
    else:
        return HuggingFaceLLM(model_name=llm_model_name)

if __name__ == "__main__":
    llm = get_inference_llm(OPENAI_MODEL_NAME)
    message = ChatMessage(
        content="What is the capital of France?",
        role=MessageRole.USER,
    )
    response = llm.infer(message)
    print(response.content)