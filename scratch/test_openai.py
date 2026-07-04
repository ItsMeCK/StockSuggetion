import os
import logging
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
logging.basicConfig(level=logging.INFO)

def test_key():
    key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    logging.info(f"Testing key: {key[:15]}... Model: {model}")
    try:
        client = OpenAI(api_key=key)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "hello"}],
            max_tokens=5
        )
        logging.info(f"API Success! Response: {response.choices[0].message.content}")
    except Exception as e:
        logging.error(f"API Failed: {e}")

if __name__ == "__main__":
    test_key()
