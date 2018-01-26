#!/usr/bin/env python3
__author__      = "Matthias Haimel"

"""
The script reads a FASTA file and creates a BED file with <chrom> <start> <end> positions.
These positions represent blocks of sequence NO complete line of Ns. 
Not precise, since some Ns remain in lines with non-N bases. 
Allows to overcome 'small' assembly issues and focuses on 'large' regions.
"""

import argparse

parser = argparse.ArgumentParser(description='Extract non-N regions from FASTA file, only if whole line are NNNs')
parser.add_argument("-f", "--fasta",  nargs=1, type=str, required=True, help="Input sequence file in FASTA format")
parser.add_argument("-o", '--output', nargs=1, type=str, required=True, help="Output file in BED format")

args = parser.parse_args()

fasta=args.fasta
outtxt=args.output
isN=True
lastN=-1
pos=0
chrom=""

def print_position():
  ofh.write("{}\t{}\t{}\n".format(chrom,lastN,pos))

with open(fasta, "r") as fh:
  with open(outtxt,"w") as ofh:
    for line in fh:
      line = line.strip()
      if line.startswith(">"):
        if not isN:
          print_position()
        chrom=line.split(" ")[0].replace(">","")
        pos=0
        lastN=0
        continue
      ncnt=0
      tcnt=len(line)
      line = line.replace("N","")
      if len(line) < 1:
        if not isN:
            isN=True
            print_position()
            lastN=pos
      elif isN:
        isN=False
        lastN=pos
      pos=pos+tcnt

    if not isN:
      print_position()
    

