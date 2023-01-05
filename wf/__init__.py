"""
Assemble and sort some COVID reads...
"""

import os
import subprocess
from pathlib import Path

from latch import small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter, LatchDir

@small_task
def blast_task(
    query: LatchFile = LatchFile(
        "s3://latch-public/test-data/6064/blast-nf/data/sample.fa"
    ),
    db: LatchDir = LatchDir("s3://latch-public/test-data/6064/blast-nf/blast-db/pdb/tiny"),
    out: str = "blast-results.txt",
) -> LatchFile:

    results_file = Path("results.txt").resolve()

    db_filenames = [file.split('.')[0] for file in os.listdir(db.local_path)]

    common_prefix = os.path.commonprefix(db_filenames)

    blast_cmd = [
        "/root/bin/micromamba",
        "run",
        "-n",
        "blast-nf",
        "/bin/bash",
        "-c",
        f"""
        nextflow run /root/blast-nf/main.nf \
        --query {query.local_path} \
        --db {db.local_path}/{common_prefix} \
        --out {str(results_file)}
        """,
    ]

    subprocess.run(blast_cmd, check=True)

    return LatchFile(str(results_file), f"latch:///{out}")


"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="Example: Wrapping a Nextflow BLAST Pipeline in Latch SDK",
    documentation="your-docs.dev",
    author=LatchAuthor(
        name="John von Neumann",
        email="hungarianpapi4@gmail.com",
        github="github.com/fluid-dynamix",
    ),
    repository="https://github.com/your-repo",
    license="MIT",
    parameters={
        "query": LatchParameter(
            display_name="FASTA File",
            description="Select FASTA file.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "db": LatchParameter(
            display_name="BLAST Database",
            description="Select the database to run BLAST against.",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "out": LatchParameter(
            display_name="Output Text File",
            description="Specify the location of the output text file.",
            batch_table_column=True,  # Show this parameter in batched mode.
        )
    },
    tags=[],
)


@workflow(metadata)
def blast_wf(
    query: LatchFile, db: LatchDir, out: str
) -> LatchFile:
    """Description...

    markdown header
    ----

    Write some documentation about your workflow in
    markdown here:

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2
    """
    return blast_task(query=query, db=db, out=out)


"""
Add test data with a LaunchPlan. Provide default values in a dictionary with
the parameter names as the keys. These default values will be available under
the 'Test Data' dropdown at console.latch.bio.
"""
LaunchPlan(
    blast_wf,
    "Test Data",
    {
        "query": LatchFile(
            "s3://latch-public/test-data/6064/blast-nf/data/sample.fa"
        ),
        "db": LatchDir("s3://latch-public/test-data/6064/blast-nf/blast-db/pdb"),
        "out": "blast-results.txt",
    },
)
