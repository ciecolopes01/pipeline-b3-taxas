import os

filepath = r"C:\Users\cieco\Documents\projeto\plgn\data\raw\2026\03\06\TS260306.ex_"

if not os.path.exists(filepath):
    print("File not found")
else:
    size = os.path.getsize(filepath)
    print(f"File size: {size}")
    with open(filepath, "rb") as f:
        head = f.read(100)
    print(f"Head: {head}")
    print(f"Head hex: {head.hex()}")
