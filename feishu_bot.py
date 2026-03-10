import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import os
import json
import time
from dotenv import load_dotenv
from collections import deque
from query_agent import run_query_agent

# Load environment variables
load_dotenv()

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")

if not APP_ID or not APP_SECRET:
    raise ValueError("FEISHU_APP_ID or FEISHU_APP_SECRET not found in .env")

class FeishuBot:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        # WebSocket client for receiving events
        self.ws_client = None
        # HTTP client for sending API requests (replies)
        self.api_client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
        # Deduplication cache for message_ids (store last 100 messages)
        self.processed_message_ids = deque(maxlen=100)
        # Record bot start time to filter out old messages (ms)
        self.start_time_ms = int(time.time() * 1000)

    def start(self):
        event_handler = lark.EventDispatcherHandler.builder("", "") \
            .register_p2_im_message_receive_v1(self.handle_message) \
            .build()

        self.ws_client = lark.ws.Client(
            self.app_id,
            self.app_secret,
            event_handler=event_handler,
            log_level=lark.LogLevel.INFO,
        )
        self.ws_client.start()

    def handle_message(self, data: P2ImMessageReceiveV1) -> None:
        message_id = data.event.message.message_id
        create_time = int(data.event.message.create_time)
        
        # 1. Deduplicate by message_id
        if message_id in self.processed_message_ids:
            print(f"[Info] Skip duplicate message: {message_id}")
            return
        self.processed_message_ids.append(message_id)

        # 2. Filter out historical messages (created before bot start)
        if create_time < self.start_time_ms:
            print(f"[Info] Skip old message (timestamp {create_time} < start {self.start_time_ms})")
            return

        message_content = data.event.message.content
        chat_id = data.event.message.chat_id
        
        try:
            content_json = json.loads(message_content)
            text = content_json.get("text", "")
        except json.JSONDecodeError:
            print(f"[Error] Failed to parse message content: {message_content}")
            return

        # Skip if message is empty or from self (though events usually filter self)
        if not text:
            return

        print(f"\n[Feishu] Received message: {text}")
        
        try:
            print("[Agent] Starting analysis workflow...")
            # The run_query_agent function prints the reasoning trace to stdout
            # which fulfills the user's requirement to see the workflow in the terminal.
            answer = run_query_agent(text)
            print(f"[Agent] Analysis complete. Sending reply...")
        except Exception as e:
            import traceback
            traceback.print_exc()
            answer = f"系统处理出错: {str(e)}"
            print(f"[Error] {answer}")

        self.reply(chat_id, answer)

    def reply(self, chat_id, text):
        request = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": text}))
                .build()
            )
            .build()
        )
        
        response = self.api_client.im.v1.message.create(request)
        if not response.success():
            print(f"[Error] Failed to reply: {response.code} - {response.msg}")
        else:
            print(f"[Feishu] Reply sent successfully.")

if __name__ == "__main__":
    print("Initializing Feishu Agentic BI Bot...")
    bot = FeishuBot(APP_ID, APP_SECRET)
    print("Bot started. Waiting for messages...")
    bot.start()
