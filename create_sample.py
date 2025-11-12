import pandas as pd

# Create a simple DataFrame
data = {
    'city': ['Kano', 'Abuja', 'Lagos', 'Kano', 'Lagos'],
    'temp': [35.2, 28.5, 30.1, 36.1, 31.0]
}
df = pd.DataFrame(data)

# Save it as a Parquet file
df.to_parquet('sample.parquet')

print("Successfully created sample.parquet")