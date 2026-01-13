
"""
run as
snakemake --unlock; snakemake -j 25 --rerun-triggers input --rerun-incomplete
"""
# NUMCHUNKS = 400
NUMCHUNKS = 1600
chunks = [f"{i:04d}" for i in range(NUMCHUNKS + 1)]
# langs = "hr sl sr cnr bs mk bg".split()
langs = [
    # "cnr",
    # "bs",
    # "sl",
    # "mk",
    # "hr",
    # "sr",
    "bg"
]
rule chunk_input:
    input: "input_data/CLASSLA-web-2024.{lang}.jsonl"
    output: [f"chunks/{{lang}}/chunk_{i}" for i in chunks]
    params:
        numchunks=NUMCHUNKS,
    shell:
        """
        output_prefix="chunks/{wildcardsx.lang}/chunk_"
        total_lines=$( srun -J "wc-ing" --time=04:00:00 wc -l < "{input}")
        echo "Got total lines: $total_lines"
        lines_per_chunk=$(( total_lines / {params.numchunks}  ))
        echo "Got lines per chunk: $lines_per_chunk"
        srun --time=24:00:00 --job-name "chunking" split -l $lines_per_chunk -d -a 4 "{input}" "$output_prefix"
        """
rule process_chunk:
    input: "chunks/{lang}/chunk_{i}"
    output: "processed_chunks/{lang}/chunk_{i}.jsonl"
    params:
        lang=lambda wildcards: {"cnr": "hr","bs":"hr"}.get(wildcards.lang, wildcards.lang)
    shell: """srun -J "{wildcards.lang}annotation" --ntasks=1  --gpus=0  --cpus-per-task=1 --mem=50000 --time=5-12:00:00 ~/miniforge3/envs/classlaenv/bin/python scripts/process_chunk.py {input} {output} {params.lang} {wildcards.lang} """

rule jsonl_to_vert:
    input: rules.process_chunk.output[0]
    output: "vert_chunks/{lang}/chunk_{i}.vert"
    shell: """srun -J verting --time=01:00:00 --ntasks=1 --cpus-per-task=1 --mem=30000  -- /ceph/home/prupnik/miniforge3/envs/conlluenv/bin/python scripts/jsonl_to_vert.py  "{input}" "{output}" "{wildcards.lang}" """



rule concat_jsonl_results:
    input: expand("processed_chunks/{{lang}}/chunk_{i}.jsonl", i = chunks)
    output: "CLASSLA-web.{lang}.2.0.jsonl"
    shell:
        """srun -J catting --time=24:00:00 --ntasks=1 --cpus-per-task=1 --gpus=1 --mem=30000 -- ~/miniforge3/bin/python scripts/concat_jsons_and_strip_nonsanitised_text.py  {output[0]} {input}"""

rule concat_vert_results:
    input: expand("vert_chunks/{{lang}}/chunk_{i}.vert", i = chunks)
    output:
        vert="CLASSLA-web.{lang}.2.0.vert",
        gzip="CLASSLA-web.{lang}.2.0.vert.gz",
    shell:
        """srun -J catting --time=24:00:00 cat {input} > {output.vert}
        # echo "Lines in the vert: $( wc -l {output.vert})"
        srun -J compressing --time=2-00:00:00  gzip -kc {output.vert} > {output.gzip}
        """
rule gather_jsons:
    input: expand(rules.concat_jsonl_results.output, lang=langs)

rule gather:
    default_target: True
    input: expand(rules.concat_vert_results.output.vert, lang=langs) #+ expand(rules.concat_jsonl_results.output, lang=langs)

rule clean:
    shell:
        """
        echo "This will clean all intermediate data (chunks, chunk jsons, chunk verts)! Are you sure? (y|N)"
        read response
        if [ $response == "y" ]
        then
            for dir in chunks processed_chunks vert_chunks
            do
                echo "Removing directory $dir"
                # rm -rf "$dir"
            done
        fi
        """