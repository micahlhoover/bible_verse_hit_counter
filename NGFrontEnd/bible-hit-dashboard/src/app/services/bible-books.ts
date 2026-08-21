/**
 * Canonical order of all 66 books of the Bible
 */
const CANONICAL_BIBLE_BOOKS: string[] = [
  // Old Testament (39 books)
  'Genesis',
  'Exodus',
  'Leviticus',
  'Numbers',
  'Deuteronomy',
  'Joshua',
  'Judges',
  'Ruth',
  '1 Samuel',
  '2 Samuel',
  '1 Kings',
  '2 Kings',
  '1 Chronicles',
  '2 Chronicles',
  'Ezra',
  'Nehemiah',
  'Esther',
  'Job',
  'Psalms',
  'Proverbs',
  'Ecclesiastes',
  'Isaiah',
  'Jeremiah',
  'Lamentations',
  'Ezekiel',
  'Daniel',
  'Hosea',
  'Joel',
  'Amos',
  'Obadiah',
  'Jonah',
  'Micah',
  'Nahum',
  'Habakkuk',
  'Zephaniah',
  'Haggai',
  'Zechariah',
  'Malachi',
  // New Testament (27 books)
  'Matthew',
  'Mark',
  'Luke',
  'John',
  'Acts',
  'Romans',
  '1 Corinthians',
  '2 Corinthians',
  'Galatians',
  'Ephesians',
  'Philippians',
  'Colossians',
  '1 Thessalonians',
  '2 Thessalonians',
  '1 Timothy',
  '2 Timothy',
  'Titus',
  'Philemon',
  'Hebrews',
  'James',
  '1 Peter',
  '2 Peter',
  '1 John',
  '2 John',
  '3 John',
  'Jude',
  'Revelation'
];

/**
 * Sorts an array of books by their canonical order
 * @param books Array of book objects with a 'name' property
 * @returns Sorted array in canonical Bible order
 */
export function sortBooksByCanonicalOrder<T extends { name: string }>(
  books: T[]
): T[] {
  return [...books].sort((a, b) => {
    const indexA = CANONICAL_BIBLE_BOOKS.indexOf(a.name);
    const indexB = CANONICAL_BIBLE_BOOKS.indexOf(b.name);

    // If both are found in canonical order, sort by their position
    if (indexA !== -1 && indexB !== -1) {
      return indexA - indexB;
    }

    // If only one is found, it comes first
    if (indexA !== -1) return -1;
    if (indexB !== -1) return 1;

    // If neither is found, maintain original order
    return 0;
  });
}

/**
 * Get the canonical Bible books list
 * @returns Array of all Bible books in canonical order
 */
export function getCanonicalBibleBooks(): string[] {
  return [...CANONICAL_BIBLE_BOOKS];
}
