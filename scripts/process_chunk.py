import tqdm
import json
import classla
import sys
import gc
from loguru import logger
from xml.sax.saxutils import unescape


sys.setrecursionlimit(10**4)
try:
    from sys import argv

    infile = argv[1]
    outfile = argv[2]
    lang = argv[3]
    corp = argv[4]
except Exception as e:
    logger.exception(e)
    logger.warning("Running in debug mode")
    infile = "chunks/cnr/chunk_0000"
    outfile = "brisi.jsonl"
    lang = "hr"
    corp = "cnr"

logger.info(f"\n{infile=}, \n{outfile=}, \n{lang=}")

try:
    classla.download(lang, type="web")
    nlp = classla.Pipeline(lang, type="web", processors="tokenize,pos,lemma")
    logger.success("Will use WEB type, alhamdu lillah!")
except Exception as e:
    # logger.exception(e)
    logger.critical("Web type not found, continuing with default:")
    classla.download(lang)
    nlp = classla.Pipeline(lang, processors="tokenize,pos,lemma")


def process_smartly(text, max_chars=10_000, chunk_chars=400):
    # Base case: text is short enough to process directly
    if len(text) <= max_chars:
        try:
            doc = nlp(text)
            return doc.to_conll()
        except Exception as e:
            logger.error(f"Error processing text of length {len(text)}: {e}")
            return ""

    # Find the best split point by analyzing the middle section
    start_idx = max(0, (len(text) - chunk_chars) // 2)
    end_idx = start_idx + chunk_chars
    middle_chunk = text[start_idx:end_idx]

    # Tokenize the middle chunk to find sentences
    try:
        chunk_doc = nlp(middle_chunk)
        sentences = chunk_doc.sentences
    except Exception as e:
        logger.warning(f"Error analyzing middle chunk: {e}")
        logger.exception(e)
        logger.warning("Falling back")
        # Fallback: split in the middle
        mid_point = len(text) // 2
        left_part = text[:mid_point]
        right_part = text[mid_point:]
        return process_smartly(left_part, max_chars, chunk_chars) + process_smartly(
            right_part, max_chars, chunk_chars
        )

    if not sentences:
        # No sentences found, fallback to middle split
        mid_point = len(text) // 2
        left_part = text[:mid_point]
        right_part = text[mid_point:]
        return process_smartly(left_part, max_chars, chunk_chars) + process_smartly(
            right_part, max_chars, chunk_chars
        )

    # Find the longest sentence in the middle chunk
    longest_sentence = max(sentences, key=lambda sent: len(sent.text))
    longest_sentence_text = longest_sentence.text

    # Find all occurrences of this sentence in the original text
    import re

    # Escape the sentence text for regex, but handle potential whitespace variations
    escaped_sentence = re.escape(longest_sentence_text)
    # Use regex to find the sentence, allowing for whitespace variations
    pattern = escaped_sentence.replace(r"\ ", r"\s+")
    matches = list(re.finditer(pattern, text))

    if not matches:
        # Sentence not found in original text (shouldn't happen normally), fallback
        mid_point = len(text) // 2
        left_part = text[:mid_point]
        right_part = text[mid_point:]
        return process_smartly(left_part, max_chars, chunk_chars) + process_smartly(
            right_part, max_chars, chunk_chars
        )

    # Find the match closest to the middle of the text
    text_mid = len(text) // 2
    best_match = min(matches, key=lambda m: abs(m.start() - text_mid))

    # Split at the end of this sentence
    split_point = best_match.end()

    # Ensure we don't create empty chunks
    left_part = text[:split_point].strip()
    right_part = text[split_point:].strip()

    if not left_part or not right_part:
        # If splitting creates empty chunks, fallback to middle split
        mid_point = len(text) // 2
        left_part = text[:mid_point]
        right_part = text[mid_point:]

    # Recursively process both parts
    left_result = process_smartly(left_part, max_chars, chunk_chars)
    right_result = process_smartly(right_part, max_chars, chunk_chars)

    return left_result + right_result


def renumber_conllu_clean(conllu_text):
    paragraphs = []
    current_paragraph = []

    lines = conllu_text.split("\n")

    for line in lines:
        if line.startswith("# newpar id = "):
            # If we have collected sentences for current paragraph, save it
            if current_paragraph:
                paragraphs.append(current_paragraph)
                current_paragraph = []
        current_paragraph.append(line)

    # Don't forget the last paragraph
    if current_paragraph:
        paragraphs.append(current_paragraph)

    # Renumber paragraphs and sentences
    output_lines = []
    for par_idx, paragraph in enumerate(paragraphs, 1):
        sent_count = 0

        for line in paragraph:
            if line.startswith("# newpar id = "):
                # Only add newpar for the first line of first sentence
                if sent_count == 0:
                    output_lines.append(f"# newpar id = {par_idx}")
            elif line.startswith("# sent_id = "):
                sent_count += 1
                output_lines.append(f"# sent_id = {par_idx}.{sent_count}")
            else:
                output_lines.append(line)

    return "\n".join(output_lines)


def deescape(s: str) -> str:
    s = unescape(s)
    s = unescape(s)

    return s


def sanitize(s: str) -> str:
    tags = "br li ul body u i b hr ref div span h1 h2 h3 p".split()
    for tag in tags:
        for prefix in ["", " ", "/", " /"]:
            for suffix in ["", " ", "/", " /"]:
                s = s.replace(f"<{prefix}{tag}{suffix}>", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s


with open(infile) as f:
    numlines = sum(1 for line in f)
logger.info(f"Will process {numlines:,d} lines")
pbar = tqdm.tqdm(total=numlines)
i = 0
from time import perf_counter

time_begin = perf_counter()
i_diff = 0
with open(infile, "r") as inf:
    with open(outfile, "w") as outf:
        line = inf.readline()
        while line:
            conll = ""
            d = json.loads(
                line.replace(f"CLASSLA-web-2024.{corp}", f"CLASSLA-web.2.0.{corp}")
            )
            text = d["text"]
            text_ds = sanitize(deescape(text))
            d["deescaped_and_sanitized_text"] = text_ds
            if len(text_ds) > 10_000:
                for paragraph in text_ds.split("\n"):
                    if paragraph.strip():
                        conll += "\n" + process_smartly(paragraph)
            else:
                conll += "\n" + process_smartly(text_ds)
            conll = renumber_conllu_clean(conll)
            d["conll"] = conll
            outf.write(json.dumps(d, ensure_ascii=False) + "\n")
            if (i % 100 == 0) or (perf_counter() - time_begin) > 10:
                pbar.update(i_diff)
                i_diff = 1
                time_begin = perf_counter()
            else:
                i_diff += 1
            i += 1
            line = inf.readline()
        print(f"File reading stopped after {i} rows")
pbar.close()

with open(infile) as f:
    innumlines = sum(1 for line in f if line)
with open(outfile) as f:
    outnumfiles = sum(1 for line in f if line)

assert innumlines == outnumfiles
