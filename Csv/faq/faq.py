import csv
import uuid




def faq_import(file_path, TYPE):
    units = []

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            units.append({
                "id": str(uuid.uuid4()),
                "question": row["question"].strip(),
                "answer": row["answer"].strip(),
                "typeFaqId": TYPE,
            })

    return units
