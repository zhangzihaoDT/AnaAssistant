如果你打算用 **长连接（WebSocket）接收回调**，那就是 **飞书的长连接事件订阅模式**。这种模式比 HTTP Webhook 更简单，因为 **不需要公网服务器**。

在 **飞书** 的架构里是这样：

```
飞书事件服务器
      ↓
WebSocket 长连接
      ↓
你的程序
```

也就是说：

- 你的程序主动连接飞书
- 飞书把事件通过 WebSocket 推送给你
- 不需要公网 URL

---

# 一、先在开放平台开启长连接

在 **飞书开放平台**：

```
事件与回调
   ↓
事件订阅
   ↓
选择 长连接模式
```

然后添加事件：

```
im.message.receive_v1
```

这样用户 **@机器人** 的消息就会推送给你。

---

# 二、官方推荐 SDK（最简单）

飞书官方提供 Python SDK：

```
pip install lark-oapi
```

示例代码（最小可运行）：

```python
import lark_oapi as lark
from lark_oapi.api.im.v1 import *

APP_ID = "你的appid"
APP_SECRET = "你的secret"

def do_p2_im_message_receive_v1(data: P2ImMessageReceiveV1) -> None:
    print("收到消息:", data.event.message.content)

event_handler = (
    lark.EventDispatcherHandler.builder("", "")
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1)
    .build()
)

client = lark.ws.Client(
    APP_ID,
    APP_SECRET,
    event_handler=event_handler,
    log_level=lark.LogLevel.DEBUG,
)

client.start()
```

运行后：

```
python bot.py
```

程序会：

```
启动 websocket
↓
连接飞书
↓
监听消息
```

只要群里有人：

```
@机器人 你好
```

终端就会打印。

---

# 三、消息内容结构

你会收到类似：

```json
{
  "event": {
    "message": {
      "chat_id": "oc_xxx",
      "content": "{\"text\":\"@bot 今天销量多少\"}",
      "message_id": "om_xxx"
    }
  }
}
```

需要解析：

```
content.text
```

---

# 四、回复消息

收到消息后调用发送 API：

```python
from lark_oapi.api.im.v1 import *

def reply(client, chat_id, text):

    request = (
        CreateMessageRequest.builder()
        .receive_id_type("chat_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("text")
            .content(f'{{"text":"{text}"}}')
            .build()
        )
        .build()
    )

    response = client.im.v1.message.create(request)
```

---

# 五、长连接模式架构

最终结构：

```
飞书群
   ↓
@机器人
   ↓
飞书事件服务器
   ↓
WebSocket
   ↓
你的 Python 程序
   ↓
LLM / SQL Agent
   ↓
飞书 API 回复
```

优点：

- 不需要服务器
- 不需要公网
- 本地 Mac 就能跑
- 非常适合开发测试

---

# 六、一个重要限制

长连接模式 **程序必须一直运行**：

```
python bot.py
```

如果程序停止：

```
事件就收不到
```

所以生产环境一般会：

```
云服务器
+ supervisor
+ docker
```
