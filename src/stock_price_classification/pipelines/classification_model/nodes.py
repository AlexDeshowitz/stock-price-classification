"""
This is a boilerplate pipeline 'classification_model'
generated using Kedro 0.18.2
"""
# base packages:
import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold

# models:
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier

# evaluation functions:
from sklearn.metrics import precision_score, recall_score, f1_score


# functions:
def train_models(X_train: pd.DataFrame, y_train: pd.DataFrame, parameters) -> pd.DataFrame:

    '''Trains a series of machine learning model outputs for evaluation by the user
    
    Args:
        X_train: inputs from train-test split function
        y_train: y-series from the train-test split function

    Returns:
        Summarized output of all ML models tried
    
    '''

    # define all of the models to be used:
    classifiers = {
    "LogisticRegression": LogisticRegression(n_jobs = -1, max_iter = 100000),
    "RandomForestClassifier": RandomForestClassifier(n_jobs = -1),
    "SVC": SVC(C = parameters['support_vector_classifier']['c'], kernel =parameters['support_vector_classifier']['kernel'], gamma = parameters['support_vector_classifier']['gamma']),
    "AdaBoostClassifier": AdaBoostClassifier()
    }

    # create a readable representation of the target:
    y_train = y_train.iloc[:, 0].values
    X_train = X_train.values

    #TODO: add precision, recall, f-measure on all sets
    #accuracies = {}
    names = []
    models = []
    fold = []
    training_samples = []

    #TODO: Rename test to validation **

    train_accuracies = []
    test_accuracies = []

    train_precisions = []
    test_precisions = []

    train_recalls = []
    test_recalls = []

    train_f_measures = []
    test_f_measures = []

    # iterate through the models:
    for name, classifier in classifiers.items():

        clf = classifier
        print(name)
        print (clf)

        # iterate through the folds: ->> not ideal to nest the loops here
        cv = StratifiedKFold(n_splits=parameters['cross_val_splits'], shuffle=True, random_state=parameters['seed']).split(X_train, y_train)

        for k, (fold_train, fold_test) in enumerate(cv):

             # append model name into list:
            models.append(str(classifier))
            
            clf.fit(X_train[fold_train],y_train[fold_train])
        

            # create predictions:
            train_pred = clf.predict(X = X_train[fold_train])
            test_pred = clf.predict(X = X_train[fold_test])

            # calculate accuracies:
            train_accuracy = clf.score(X_train[fold_train], y_train[fold_train])
            test_accuracy = clf.score(X_train[fold_test], y_train[fold_test])
    
            # calculate precision:
            train_precision = precision_score(y_train[fold_train], train_pred)
            test_precision = precision_score(y_train[fold_test], test_pred)
 
            # calculate recall:
            train_recall = recall_score(y_train[fold_train], train_pred)
            test_recall = recall_score(y_train[fold_test], test_pred)

            # calculate f-measure:
            train_f = f1_score(y_train[fold_train], train_pred)
            test_f = f1_score(y_train[fold_test], test_pred)
            

            # append name:
            names.append(name)
            
            # append training sample size:
            training_samples.append( len(X_train[fold_train]) )

            # append fold number to the list:
            fold.append(k+1)

            # append score into list:
            train_accuracies.append(train_accuracy)
            test_accuracies.append(test_accuracy)

            # append precisions to the list:
            train_precisions.append(train_precision)
            test_precisions.append(test_precision)

            # append recalls to the list:
            train_recalls.append(train_recall)
            test_recalls.append(test_recall)

            # append f-measures to the list:
            train_f_measures.append(train_f)
            test_f_measures.append(test_f)
    
    results_df = pd.DataFrame({
                "names" : names,
                "model" : models,
                "fold" : fold,
                "training_samples" : training_samples,
                "train_accuracy": train_accuracies,
                "test_accuracy": test_accuracies,
                "train_precision": train_precisions,
                "test_precision": test_precisions,
                "train_recall": train_recalls,
                "test_recall": test_recalls,
                "train_f_measures": train_f_measures,
                "test_f_measures": test_f_measures
                
                })

    # create aggregated results df:
    aggregated_results_df = results_df.drop(columns = ['fold']).groupby(by = ['names', 'model']).mean()
                            
    
    return results_df, aggregated_results_df
