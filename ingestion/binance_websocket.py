import json

try:
    import websocket
except Exception:  # pragma: no cover - dependency missing at runtime
    websocket = None





def run_binance_socket(message_handler):
    if websocket is None:
        raise RuntimeError(
            "websocket-client package is required. Install with: pip install websocket-client"
        )
        
    def on_message(ws, message):
        data = json.loads(message)
        message_handler(data)
    
    socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    ws = websocket.WebSocketApp(socket, on_message=on_message)
    
    print("🚀 Binance WebSocket started...")
    ws.run_forever()


if __name__ == "__main__":
    try:
        def print_handler(data):
            print(data)
        run_binance_socket(print_handler)
    except RuntimeError as e:
        print(e)