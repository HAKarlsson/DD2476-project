#!/usr/bin/env bash

#python3.6 indexer.py --dataset dataset 
python3.6 feature_extraction.py --days 25:27 --output training_data
python3.6 feature_extraction.py --days 28:30 --output test_data --test
java -jar RankLib.jar -train training_data -ranker 6 -gmax 2 -metric2t NDCG@10 -save model.mdl
java -jar RankLib.jar -load model.mdl -rank test_data -score reranked_test_data
python3.6 ranklib2kaggle.py test_data reranked_test_data kaggle_file