# classla_web_anno_pipeline
Code for post-processing crawled corpora


## Setup

The pipeline was run on a cluster computer running SLURM. `mamba` was used for
python environment management:
+ base environment: for running the pipeline, as exported in [baseenv.yml
  file](baseenv.yml)
+ classla environment: for annotating text, exported to
  [classlaenv.yml](classlaenv.yml)
+ conllu environment: for constructing VERT files, defined in [file
  conlluenv.yml](conlluenv.yml)

Environments can be recreated by running `mamba env create -f environment.yml`,
but note that these environments are tailor made to available hardware, which
means that especially all CUDA-related packages might not be compatible with
other computing infrastructure.

## Pipeline description

As evident on [this plot](rulegraph.pdf), the pipeline does the following:
+ Chunk the input files into N pieces (here we used 20 for
  shorter corpora, or up to 1600 for the largest corpora)
+ Every chunk is processed with
  [Classla-Stanza](https://pypi.org/project/classla/) (using the web type
  wherever possible), and the results are added to jsonls.
+ Every processed chunk is transformed into a vert file
+ Finally, processed chunks are merged together into a master JSONL, and vert
  files are merged into a master VERT

## Directory structure

The pipeline expects input data in the `input_data` directory. It will
automatically create directories `chunks`, `processed_chunks`, `vert_chunks`.

```
.
├── input_data
│   ├── CLASSLA-web-2024.bg.jsonl
│   ├── CLASSLA-web-2024.bs.jsonl
│   ├── CLASSLA-web-2024.cnr.jsonl
│   ├── CLASSLA-web-2024.hr.jsonl
│   ├── CLASSLA-web-2024.mk.jsonl
│   ├── CLASSLA-web-2024.sl.jsonl
│   └── CLASSLA-web-2024.sr.jsonl
└── scripts
    ├── jsonl_to_vert.py
    └── process_chunk.py
```