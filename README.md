# VILMAA
Author: Matthias Haimel

The VILMAA package allows to __load__ Illumina single sample whole genome variation files (gVCF) into Hadoop and provides incremental __merge__, __annotate__ as well as precalculate allele frequencies for scalabel __analysis__. Analysis ready genome variation data can be exported for different releases in AVRO format and interrogated using SPARK SQL for efficient proformance. 

A typical query filtering ~350 million variants including ~13K sample genomes for rare ( allelle frequency less than 1 in 10,000) and highly disruptive (protein truncating variants) only takes a minute.


