

#   run it like this :
#   python pull_d3.py
#       (this is a refactored pull_meta.py)

#   Get every book in ./archive
#   Gather the book TOTAL hits
#   Gather each chapters TOTAL hits
#   TODO : make a normalize script later

import json
from pathlib import Path
import csv
from typing import Dict, List

path_to_raw_canonical_order_verse_totals = "./archive"


BIBLE_CANON_ORDER = [
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
    "Joshua", "Judges", "Ruth",
    "1 Samuel", "2 Samuel", "1 Kings", "2 Kings",
    "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther",
    "Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Solomon",
    "Isaiah", "Jeremiah", "Lamentations", "Ezekiel", "Daniel",
    "Hosea", "Joel", "Amos", "Obadiah", "Jonah", "Micah", "Nahum",
    "Habakkuk", "Zephaniah", "Haggai", "Zechariah", "Malachi",
    "Matthew", "Mark", "Luke", "John", "Acts",
    "Romans", "1 Corinthians", "2 Corinthians", "Galations",
    "Ephesians", "Philippians", "Colossians", "1 Thessalonians",
    "2 Thessalonians", "1 Timothy", "2 Timothy", "Titus",
    "Philemon", "Hebrews", "James", "1 Peter", "2 Peter",
    "1 John", "2 John", "3 John", "Jude", "Revelation"
]

 
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


def to_d3_hierarchy(books_dict, root_name="Bible"):
    children = []

    for book_key, book in books_dict.items():
        book_verse_total = int(book.book_verse_hit_total)

        # ✅ THIS is the chapter map
        ch_map = book.chapter_verse_hit_total or {}

        chap_children = [
            {
                "name": f"Chapter {chap}",
                "size": int(hits)
            }
            for chap, hits in sorted(ch_map.items(), key=lambda kv: int(kv[0]))
        ]

        children.append({
            "name": book.bookName or book_key,
            "children": chap_children,
            "book_verse_hit_total": book_verse_total
        })

    # ✅ No sorting → preserves canonical input order
    return {
        "name": root_name,
        "children": children
    }

def main():

    print(f"path to files : {path_to_raw_canonical_order_verse_totals}")

    files = []

    for bible_book_name in BIBLE_CANON_ORDER :
        # e.g. Matthew_total_hits_dataforseo_repaired.csv
        file_candidate = f"{bible_book_name}_total_hits_dataforseo_repaired.csv"
        if Path(f"{path_to_raw_canonical_order_verse_totals}/{file_candidate}").exists():
            files.append(file_candidate)
            print(f"appending : {file_candidate}")

    #files = [f.name for f in Path(path_to_raw_canonical_order_verse_totals).iterdir() if f.is_file()]
    
    print(f"files : {files}")

    print(f"Books of the bible count : {len(BIBLE_CANON_ORDER)}")

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

    d3_ready = to_d3_hierarchy(books)
    
    with open("books_d3.json", "w", encoding="utf-8") as f:
        json.dump(d3_ready, f, indent=2, ensure_ascii=False)

    print('Paste books_d3.json into the "data = " section of ./d3_scratch/bible_hits_hierarchical_bar_chart.html')


if __name__ == "__main__":

    main()