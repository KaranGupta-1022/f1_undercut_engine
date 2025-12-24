import fastf1
import pandas as pd
import numpy as np
from kafka import KafkaProducer

print("=" * 50)
print(" F1 UNDERCUT ENGINE - SETUP TEST")
print("=" * 50)

# Test FastF1
print("\nFastF1 imported successfully!")
print(f"   Version: {fastf1.__version__}")

# Test Pandas
print("\nPandas imported successfully!")
print(f"   Version: {pd.__version__}")

# Test NumPy
print("\nNumPy imported successfully!")
print(f"   Version: {np.__version__}")

# Test Kafka (will fail to connect, but import should work)
print("\nKafka-Python imported successfully!")
print("\n" + "=" * 50)
print("ALL DEPENDENCIES INSTALLED CORRECTLY!")
print("=" * 50)
print("\nYou're ready to start building!")