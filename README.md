## Automated mass spectrometry file retrieval and conversion with Snakemake

The first step in most re-analyses of public data is downloading and converting the files.
This module is focused completely on that step, with hopes to simplify the process.
In the end, I hope that this module will accomplish three tasks:

 - Automatic file discovery in a repository
 - Conversion of the files to mzML or MGF if not already there
 - Standard quality control of individual files
