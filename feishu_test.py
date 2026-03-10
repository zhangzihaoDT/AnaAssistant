import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")

if not APP_ID or not APP_SECRET:
    raise ValueError("FEISHU_APP_ID or FEISHU_APP_SECRET not found in .env")

def do_p2_im_message_receive_v1(data: P2ImMessageReceiveV1) -> None:
    # Print the received message content
    print(f"[Feishu] Received message: {data.event.message.content}")
    
    # You can process the message here (e.g., send to LLM)
    # content is a JSON string, e.g., '{"text":"@bot hello"}'

def main():
    event_handler = lark.EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
        .build()

    client = lark.ws.Client(
        APP_ID,
        APP_SECRET,
        event_handler=event_handler,
        log_level=lark.LogLevel.DEBUG,
    )

    print("Starting Feishu WebSocket client...")
    client.start()

if __name__ == "__main__":
    main()
