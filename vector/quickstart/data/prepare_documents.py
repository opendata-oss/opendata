"""Fetch and chunk two full books into documents.json for the vector quickstart.

Book 1: "The Rust Programming Language" - fetched from doc.rust-lang.org/book/print.html
Book 2: "The Up-To-Date Sandwich Book" - fetched from Project Gutenberg

Run this script once offline, then commit the output documents.json.

Usage:
    python prepare_documents.py
"""

import json
import re
from html.parser import HTMLParser
from urllib.request import urlopen, Request

USER_AGENT = "OpenData-Vector-Quickstart/1.0"


# ---------------------------------------------------------------------------
# Rust Book
# ---------------------------------------------------------------------------

# Known chapter-level heading IDs (these are chapter intros, not content sections)
RUST_CHAPTER_IDS = {
    "the-rust-programming-language",
    "foreword",
    "introduction",
    "getting-started",
    "common-programming-concepts",
    "understanding-ownership",
    "using-structs-to-structure-related-data",
    "enums-and-pattern-matching",
    "packages-crates-and-modules",
    "common-collections",
    "error-handling",
    "generic-types-traits-and-lifetimes",
    "writing-automated-tests",
    "an-io-project-building-a-command-line-program",
    "functional-language-features-iterators-and-closures",
    "more-about-cargo-and-cratesio",
    "smart-pointers",
    "fearless-concurrency",
    "fundamentals-of-asynchronous-programming-async-await-futures-and-streams",
    "object-oriented-programming-features",
    "patterns-and-matching",
    "advanced-features",
    "final-project-building-a-multithreaded-web-server",
    "appendix",
}

# Map chapter heading IDs to friendly chapter names
RUST_CHAPTER_NAMES = {
    "getting-started": "1. Getting Started",
    "common-programming-concepts": "3. Common Programming Concepts",
    "understanding-ownership": "4. Understanding Ownership",
    "using-structs-to-structure-related-data": "5. Using Structs",
    "enums-and-pattern-matching": "6. Enums and Pattern Matching",
    "packages-crates-and-modules": "7. Packages, Crates, and Modules",
    "common-collections": "8. Common Collections",
    "error-handling": "9. Error Handling",
    "generic-types-traits-and-lifetimes": "10. Generic Types, Traits, and Lifetimes",
    "writing-automated-tests": "11. Writing Automated Tests",
    "an-io-project-building-a-command-line-program": "12. An I/O Project",
    "functional-language-features-iterators-and-closures": "13. Functional Language Features",
    "more-about-cratesio": "14. More About Cargo and Crates.io",
    "smart-pointers": "15. Smart Pointers",
    "fearless-concurrency": "16. Fearless Concurrency",
    "fundamentals-of-asynchronous-programming-async-await-futures-and-streams": "17. Async and Await",
    "object-oriented-programming-features": "18. Object-Oriented Programming",
    "patterns-and-matching": "19. Patterns and Matching",
    "advanced-features": "20. Advanced Features",
    "final-project-building-a-multithreaded-web-server": "21. Final Project",
    "appendix": "Appendix",
    "introduction": "Introduction",
    "foreword": "Foreword",
    "programming-a-guessing-game": "2. Programming a Guessing Game",
}


class RustPrintPageParser(HTMLParser):
    """Parse the Rust book print.html, splitting on h1 tags."""

    def __init__(self):
        super().__init__()
        self.sections = []  # (id, title_text, body_text)
        self.current_id = None
        self.current_title = ""
        self.text_parts = []
        self.in_h1 = False
        self.h1_parts = []
        self.skip_depth = 0
        self.skip_tags = {"script", "style", "nav", "pre", "code"}
        self.in_content = False  # True after we see the first h1

    def _flush(self):
        if self.current_id is not None:
            text = " ".join(self.text_parts)
            text = re.sub(r"\s+", " ", text).strip()
            self.sections.append((self.current_id, self.current_title, text))

    def handle_starttag(self, tag, attrs):
        if tag in self.skip_tags:
            self.skip_depth += 1
            return

        if tag == "h1":
            attrs_dict = dict(attrs)
            h1_id = attrs_dict.get("id", "")
            if h1_id:
                self._flush()
                self.current_id = h1_id
                self.text_parts = []
                self.in_h1 = True
                self.h1_parts = []
                self.in_content = True

    def handle_endtag(self, tag):
        if tag in self.skip_tags and self.skip_depth > 0:
            self.skip_depth -= 1
            return

        if tag == "h1" and self.in_h1:
            self.in_h1 = False
            self.current_title = " ".join(self.h1_parts).strip()

    def handle_data(self, data):
        if self.skip_depth > 0:
            return
        if self.in_h1:
            self.h1_parts.append(data)
        if self.in_content and self.current_id is not None:
            self.text_parts.append(data)

    def finish(self):
        self._flush()
        return self.sections


def fetch_rust_book():
    """Fetch The Rust Programming Language from the print page."""
    print("  Fetching print.html (all chapters in one page)...")
    url = "https://doc.rust-lang.org/book/print.html"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    html = urlopen(req, timeout=60).read().decode("utf-8")
    print(f"  Downloaded {len(html)} bytes")

    parser = RustPrintPageParser()
    parser.feed(html)
    raw_sections = parser.finish()
    print(f"  Found {len(raw_sections)} h1 sections")

    documents = []
    current_chapter = "Introduction"
    section_counter = 0

    for section_id, title, text in raw_sections:
        # Update chapter if this is a chapter heading
        if section_id in RUST_CHAPTER_IDS:
            chapter_name = RUST_CHAPTER_NAMES.get(section_id, title)
            current_chapter = chapter_name
            # Skip chapter intro pages (they're usually very short)
            if len(text) < 200:
                continue

        # Skip appendix sections and very short sections
        if section_id.startswith("a---") or section_id.startswith("b---") or section_id.startswith("c---"):
            continue
        if section_id.startswith("d---") or section_id.startswith("e---") or section_id.startswith("f---"):
            continue
        if section_id.startswith("g---"):
            continue
        if len(text) < 100:
            continue

        # Clean title
        clean_title = title.replace("\u00a7", "").strip()

        section_counter += 1
        doc_id = f"rust-{section_id}"

        documents.append(
            {
                "id": doc_id,
                "text": text,
                "metadata": {
                    "book": "The Rust Programming Language",
                    "chapter": current_chapter,
                    "section": clean_title,
                },
            }
        )

    print(f"  Produced {len(documents)} documents")
    return documents


# ---------------------------------------------------------------------------
# Sandwich Book
# ---------------------------------------------------------------------------

SANDWICH_CHAPTERS = {
    "FISH", "EGG", "SALAD", "MEAT", "CHEESE", "NUT",
    "SWEET", "MISCELLANEOUS", "CANAPES",
}


def fetch_sandwich_book():
    """Fetch and parse The Up-To-Date Sandwich Book from Project Gutenberg."""
    url = "https://www.gutenberg.org/cache/epub/75893/pg75893.txt"
    print(f"  Fetching {url}...")

    req = Request(url, headers={"User-Agent": USER_AGENT})
    raw_text = urlopen(req, timeout=30).read().decode("utf-8")

    # Extract content between Gutenberg markers
    start_marker = "*** START OF THE PROJECT GUTENBERG EBOOK"
    end_marker = "*** END OF THE PROJECT GUTENBERG EBOOK"
    start_idx = raw_text.find(start_marker)
    end_idx = raw_text.find(end_marker)
    if start_idx >= 0:
        raw_text = raw_text[raw_text.index("\n", start_idx) + 1 :]
    if end_idx >= 0:
        raw_text = raw_text[: raw_text.find(end_marker)]

    documents = []
    current_chapter = "Introduction"
    current_recipe = ""
    current_text_lines = []
    seen_ids = set()

    # Recipe titles: _RECIPE NAME_
    recipe_title_re = re.compile(r"^_([A-Z][A-Z\s,.'()\-&;]+)_$")

    lines = raw_text.split("\n")

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("[Illustration"):
            continue
        if stripped == "THE UP-TO-DATE SANDWICH BOOK":
            continue

        # Chapter headings
        if stripped in SANDWICH_CHAPTERS:
            _save_recipe(documents, seen_ids, current_chapter, current_recipe, current_text_lines)
            current_recipe = ""
            current_text_lines = []
            current_chapter = stripped.title() + " Sandwiches"
            continue

        # Recipe titles
        m = recipe_title_re.match(stripped)
        if m:
            _save_recipe(documents, seen_ids, current_chapter, current_recipe, current_text_lines)
            current_recipe = m.group(1).strip().title()
            current_text_lines = []
            continue

        # Accumulate text
        if stripped and current_recipe:
            current_text_lines.append(stripped)

    _save_recipe(documents, seen_ids, current_chapter, current_recipe, current_text_lines)

    print(f"  Parsed {len(documents)} recipes")
    return documents


def _save_recipe(documents, seen_ids, chapter, recipe, text_lines):
    if not recipe or not text_lines:
        return
    text = " ".join(text_lines).strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) < 20:
        return

    slug = re.sub(r"[^a-z0-9]+", "-", recipe.lower()).strip("-")
    doc_id = f"sandwich-{slug}"
    if doc_id in seen_ids:
        counter = 2
        while f"{doc_id}-{counter}" in seen_ids:
            counter += 1
        doc_id = f"{doc_id}-{counter}"
    seen_ids.add(doc_id)

    documents.append(
        {
            "id": doc_id,
            "text": text,
            "metadata": {
                "book": "The Sandwich Book",
                "chapter": chapter,
                "section": recipe,
            },
        }
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Preparing documents for the vector quickstart...")
    print()

    print("Fetching The Rust Programming Language...")
    rust_docs = fetch_rust_book()
    print(f"  Total: {len(rust_docs)} sections")
    print()

    print("Fetching The Up-To-Date Sandwich Book...")
    sandwich_docs = fetch_sandwich_book()
    print(f"  Total: {len(sandwich_docs)} recipes")
    print()

    all_docs = rust_docs + sandwich_docs
    print(f"Total documents: {len(all_docs)}")

    output_path = "documents.json"
    with open(output_path, "w") as f:
        json.dump(all_docs, f, indent=2)
    print(f"Written to {output_path}")

    print("\nSummary:")
    books = {}
    for doc in all_docs:
        book = doc["metadata"]["book"]
        books[book] = books.get(book, 0) + 1
    for book, count in sorted(books.items()):
        print(f"  {book}: {count} documents")

    print("\nSample Rust documents:")
    for doc in rust_docs[:3]:
        print(f"  {doc['id']}: {doc['metadata']['section']} ({len(doc['text'])} chars)")
    print("\nSample Sandwich documents:")
    for doc in sandwich_docs[:3]:
        print(f"  {doc['id']}: {doc['metadata']['section']} ({len(doc['text'])} chars)")


if __name__ == "__main__":
    main()
