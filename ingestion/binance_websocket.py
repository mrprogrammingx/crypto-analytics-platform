import json

try:
    import websocket
except Exception:  # pragma: no cover - dependency missing at runtime
    websocket = None


def on_message(ws, message):
    data = json.loads(message)
    print(data)


def run_binance_socket():
    if websocket is None:
        raise RuntimeError(
            "websocket-client package is required. Install with: pip install websocket-client"
        )

    socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    ws = websocket.WebSocketApp(socket, on_message=on_message)
    ws.run_forever()


if __name__ == "__main__":
    try:
        run_binance_socket()
    except RuntimeError as e:
        print(e)