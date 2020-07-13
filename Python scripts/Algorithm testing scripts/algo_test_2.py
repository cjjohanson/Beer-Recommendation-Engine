import pandas as pd
from surprise import Dataset
from surprise import Reader
from surprise.model_selection import train_test_split, cross_validate
from surprise import accuracy
from surprise import SVD, SVDpp, SlopeOne, NMF, KNNBaseline, KNNBasic, KNNWithMeans, KNNWithZScore, BaselineOnly, NormalPredictor, CoClustering

ratings_test = pd.read_csv('/Users/chrisjohanson/Desktop/Capstone 2/ratings.csv').set_index('rating_id')
ratings_test = ratings_test.sample(frac=1)[:200000]
print('----------Data is ready----------')

#Load the dataset, save raw ratings to variable
reader = Reader(rating_scale=(1.0, 5.0))
dataset_test = Dataset.load_from_df(ratings_test, reader)

#put together list of algorithms to test out (1 out of 3 lists total)
algorithms2 = algorithms2 = [KNNBasic(), KNNWithMeans()]
#create empty list to store results data
benchmark = []

#iterate through each algorithm and save results info to benchmark
for algo in algorithms2:
    results = cross_validate(algo, dataset_test, measures=['RMSE', 'MAE'], cv=3, verbose=False)
    tmp = pd.DataFrame.from_dict(results).mean(axis=0)
    tmp = tmp.append(pd.Series([str(algo).split(' ')[0].split('.')[-1]], index=['Algorithm']))
    benchmark.append(tmp)
    print(f"{algo} complete")

#create df with the results
results_df = pd.DataFrame(benchmark).set_index('Algorithm').sort_values('test_rmse')
#save the results as a csv
results_df.to_csv('/Users/chrisjohanson/Desktop/results_df2.csv')
print('results_df2 complete')
