import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", type=int, default=502)
args = parser.parse_args()

client = AsyncModbusTcpClient(args.host, port=args.port)
