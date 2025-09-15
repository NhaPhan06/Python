import pandas as pd

# Đọc file CSV gốc
df = pd.read_csv('US.csv')

# Xóa các bản ghi trùng dựa trên name + parent_id, giữ lại bản ghi đầu tiên
df_unique = df.drop_duplicates(subset=['name', 'parent_id'], keep='first')

# Xuất ra file CSV mới
df_unique.to_csv('US_2.csv', index=False)

print("Đã xóa các bản ghi trùng name + parent_id và lưu vào US_2.csv")
