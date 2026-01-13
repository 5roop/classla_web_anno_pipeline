import json
import conllu
from loguru import logger
import tqdm

try:
    from sys import argv

    infile = argv[1]
    outfile = argv[2]
    lang = argv[3]
except IndexError:
    logger.warning("Running in debug mode")
    infile = "brisi.jsonl"
    outfile = "brisi.vert"
    lang = "cnr"
logger.info(f"Infile: {infile}")
logger.info(f"Outfile: {outfile}")
logger.info(f"lang: {lang}")

with open(infile) as f:
    numlines = sum(1 for line in f)
pbar = tqdm.tqdm(total=numlines)
with open(outfile, "w") as of:
    with open(infile, "r") as f:
        line = f.readline()
        while line:
            pbar.update(1)
            d = json.loads(line)
            id = (
                d["id"]
                .strip()
                .replace(f"CLASSLA-web-2024.{lang}", f"CLASSLA-web.2.0.{lang}")
            )
            title = d["title"].strip()
            domain = d["domain"].strip()
            tld = d["tld"].strip()
            crawl_year = str(d["crawl_year"]).strip()
            lang = d["lang"].strip()
            if ("script" in d) and (lang in ["cnr", "sr", "bs"]):
                script_str = f''' script="{d["script"].strip()}"'''
            else:
                script_str = ""

            genre = d["genre"].strip()
            topic = d["topic"].strip()
            url = d["url"].strip()
            of.write(
                f"""<text id="{id}" title="{title}" url="{url}" domain="{domain}" tld="{tld}" genre="{genre}" topic="{topic}"{script_str}>\n"""
            )
            parsed = conllu.parse(d["conll"])
            paragraph_open = False
            for paragraph in parsed:
                if "newpar id" in paragraph.metadata:
                    if paragraph_open:
                        of.write("""</p>\n""")
                    newparid = paragraph.metadata["newpar id"]
                    of.write(f"""<p id="{id}.{newparid}">\n""")
                    paragraph_open = True
                sentid = paragraph.metadata["sent_id"]
                of.write(f"""<s id="{id}.{sentid}">\n""")
                for token in paragraph:
                    form = token["form"]
                    lemma = token["lemma"]
                    upos = token["upos"]
                    xpos = token["xpos"]
                    feats = token["feats"]
                    deps = token["deps"]
                    tokid = token["id"]
                    lempos = lemma + "-" + str(xpos)[0].lower()
                    if token["misc"] is None:
                        spaceafter = "No"
                    else:
                        spaceafter = (
                            token.get(
                                "misc",
                            ).get("SpaceAfter", "probably")
                            == "No"
                        )
                    if feats is None:
                        feats_str = "_"
                    else:
                        feats_str = "|".join(
                            [f"{key}={value}" for key, value in feats.items()]
                        )

                    of.write(
                        f"""{form}\t{lempos}\t{xpos}\t{upos}\t{feats_str}\t{tokid}\n"""
                    )
                    if spaceafter == True:
                        of.write("<g/>\n")

                of.write("""</s>\n""")

            2 + 2

            of.write("""</p>\n""")
            paragraph_open = False
            of.write("</text>\n")
            line = f.readline()
            of.flush()
pbar.close()
