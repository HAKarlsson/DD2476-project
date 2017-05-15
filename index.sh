#!/usr/bin/env bash
# Author: Henrik Karlsson
#

python3.6 indexer.py --dataset dataset/test  --redo
python3.6 indexer.py --dataset dataset/train0
python3.6 indexer.py --dataset dataset/train1
python3.6 indexer.py --dataset dataset/train2
python3.6 indexer.py --dataset dataset/train3
