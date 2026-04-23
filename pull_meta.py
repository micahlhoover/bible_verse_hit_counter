

#   run it like this :
#   python pull_meta.py

#   Get every book in ./archive
#   TODO : make sure it includes "repaired" in the file name
#   Gather the book TOTAL hits
#   Gather each chapters TOTAL hits
#   TODO : make a normalize script later

import json
from pathlib import Path
import csv
from typing import Dict, List

path_to_raw_canonical_order_verse_totals = "./archive"
 
class Book:
    # Constructor method to initialize instance attributes
    def __init__(self):
        self.bookName = ""
        self.book_verse_hit_total = 0
        self.chapter_verse_hit_total : dict[int, int] = {}

    # this is necessary to serialize as json (sadly)
    def to_dict(self) -> dict:
        return {
            "bookName": self.bookName,
            "book_verse_hit_total": self.book_verse_hit_total,
            # JSON keys must be strings
            "chapter_verse_hit_total": {
                str(k): v for k, v in self.chapter_verse_hit_total.items()
            },
        }

# untested
def to_d3_hierarchy(books_dict, root_name="Bible"):
    children = []
    for book_key, book in books_dict.items():
        ch_map = book.get("chapter_verse_hit_total", {}) or {}
        chap_children = [
            {"name": f"Chapter {chap}", "size": int(hits)}
            for chap, hits in sorted(ch_map.items(), key=lambda kv: int(kv[0]))
        ]
        children.append({
            "name": book.get("bookName") or book_key,
            "children": chap_children,
            "book_verse_hit_total": int(book.get("book_verse_hit_total", 0))
        })

    children.sort(key=lambda b: b.get("book_verse_hit_total", 0), reverse=True)
    return {"name": root_name, "children": children}


def main():

    print(f"path to files : {path_to_raw_canonical_order_verse_totals}")
    files = [f.name for f in Path(path_to_raw_canonical_order_verse_totals).iterdir() if f.is_file()]
    print(f"files : {files}")

    books : Dict[str, Book] = {}

    for file in files :

        print(f"file : {file}")

        total_hits_this_book = 0

        this_book = Book()

        if "repair" not in file :
            print(f"skipping file {file} because it doesn't look repaired yet")
            continue 

        with open(f"{path_to_raw_canonical_order_verse_totals}/{file}", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = list(reader)
            book_name = ""
            
            for row in rows :
                #print(f"row : {row}")
                this_book.bookName = row["book"]
                #print(f"the book is named {this_book.bookName}")
                book_name = this_book.bookName
#                this_book.book_verse_hit_total += int(row["raw_hit_count_initial"])
                chapter = int(row["chapter"])
                hits = row["raw_hit_count_initial"]
                this_book.chapter_verse_hit_total[chapter] = int(hits)
                total_hits_this_book += int(hits)

            this_book.book_verse_hit_total = total_hits_this_book

            books[book_name] = this_book

        # now we should have total and book and chapter totals
        for key in books :
            print(f"Here is the meta on {key}")
            print(f"total verses : {books[key].book_verse_hit_total}")
            print(f"Chapter 1 of {key} has {books[key].chapter_verse_hit_total[1]} many web search hits")



    
    json_ready = {
        book_key: book.to_dict()
        for book_key, book in books.items()
    }


    with open("books_meta.json", "w", encoding="utf-8") as f:
        json.dump(json_ready, f, indent=2, ensure_ascii=False)




if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: python repair_suspicious_rows.py <csv_file>")
    #     sys.exit(1)

    main()