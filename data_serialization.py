import pandas as pd
import os

STORAGE_DIR = "yfdata"

def seralize(path):
    """
    Reads a Parquert file and converts it to text document 
    for RAG embeddings.
    """
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"Error reading {path} ; {e}")
        return []

    docs = []

    ticker = df["_meta_ticker"].iloc[0]
    report_type = df["_meta_report_type"].iloc[0]
    source = df["_meta_source"].iloc[0]
    fetched_at = df["_meta_fetched_at"].iloc[0]

    for index, row in df.iterrows():
        content_data = row.drop([c for c in row.index if c.startswith("_meta")])
        date_str = str(row.get("Date", "Unknown Date"))
        text_parts = [f"Financial Report for {ticker} ({report_type}) on {date_str}:"]
        
        for col, val in content_data.items():
            if pd.isna(val):
                continue

            if isinstance(val, (int, float)) and val > 1_000_000:
                val_str = f"{val:.0f}"
            else:
                val_str = str(val)

            text_parts.append(f"-{col}:{val_str}")
        
        final_text = "\n".join(text_parts)

        doc = {
            "id": f"{ticker}_{report_type}_{index}", # Unique ID for Pinecone
            "text": final_text,
            "metadata": {
                "ticker": ticker,
                "report_type": report_type,
                "date": date_str,
                "source": source,
                "fetched_at": fetched_at
            }
        }
        docs.append(doc)
    return docs

if __name__ == "__main__":
    parquet_files = os.listdir(STORAGE_DIR)
    all_docs = []

    for file in parquet_files:
        docs = seralize(os.path.join(STORAGE_DIR, file))
        all_docs.extend(docs)
        print(f"Processed {file}")

        print(all_docs[0]["text"])
        print("-" * 60)
        print("Metadata:",all_docs[0]["metadata"])