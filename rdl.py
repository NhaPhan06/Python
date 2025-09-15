import os
import requests
from bs4 import BeautifulSoup

base_url = "http://p3823.his2.one:6060"
page_url = f"{base_url}/Resources/Reports/74254"   # Trang gốc chứa link

save_dir = "rdl_files"
os.makedirs(save_dir, exist_ok=True)

# B1: lấy HTML
resp = requests.get(page_url, verify=False)
soup = BeautifulSoup(resp.text, "html.parser")

# B2: tìm tất cả link .rdl
rdl_links = []

valid_exts = (".rdl", ".rdlc", ".rdlx")

for a in soup.find_all("a", href=True):
    if a['href'].lower().endswith(valid_exts):
        rdl_links.append(a['href'])


print(f"Tìm thấy {len(rdl_links)} file .rdl")

# B3: tải về từng file
for link in rdl_links:
    if not link.startswith("http"):
        full_url = base_url + link
    else:
        full_url = link

    filename = os.path.basename(link)
    filepath = os.path.join(save_dir, filename)

    print(f"⬇️ Downloading {full_url} -> {filepath}")
    r = requests.get(full_url, verify=False)
    if r.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(r.content)
    else:
        print(f"❌ Failed {r.status_code}: {full_url}")
