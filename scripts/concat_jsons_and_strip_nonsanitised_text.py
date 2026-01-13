import polars as pl
from xml.sax.saxutils import unescape
from loguru import logger

for lang in [
    "cnr",
    "bs",
    "sl",
    "mk",
    "hr",
    "sr",
    "bg",
]:
    logger.info(f"Doing {lang}")
    # print(f"Doing {lang}")
    N = dict(bg=1600, cnr=20).get(lang, 400)
    infiles = [f"processed_chunks/{lang}/chunk_{i:04}.jsonl" for i in range(N + 1)]
    outfile = f"CLASSLA-web.{lang}.2.0.jsonl"
    dfs = []
    for i in infiles:
        logger.info(i)
        df = (
            pl.read_ndjson(i)
            .drop(
                [
                    "text",
                ]
            )
            .rename({"deescaped_and_sanitized_text": "text"})
            .with_columns(
                pl.col("title")
                .map_elements(lambda s: unescape(unescape(s)), return_dtype=pl.String)
                .alias("title")
            )
        )
        dfs.append(df)
    df = pl.concat(dfs)
    df.write_ndjson(outfile.replace(".jsonl", ".anno.jsonl"))
    df.drop("conll").write_ndjson(outfile)
