import random
import mlflow
import time
from argparse import ArgumentParser

def log_tags():
    mlflow.set_tag("team", random.choice(["data", "develop", "train", "pham"]))
    mlflow.set_tag("project", random.choice(["mlflow", "pipelines", "automl", "templates"]))
    mlflow.set_tag("user", random.choice(["john", "ethan", "elaine", "jennifer"]))

def log_params():
    batch_size = random.choice([8, 16, 32, 64, 128, 254])
    learning_rate = random.choice([0.1, 0.01, 0.001, 0.2, 0.02, 0.002, 0.3, 0.03, 0.003, 0.4, 0.04, 0.004, 0.5, 0.05, 0.005, 0.6, 0.06, 0.006, 0.7, 0.07, 0.007, 0.8, 0.08, 0.008, 0.9, 0.09, 0.009])
    drop_out = round(random.uniform(0.25, 0.75), 1)
    kernel_size = random.choice([3, 6, 9, 12, 15, 30])
    stopped_epoch = random.randint(50, 100)
    activation = random.choice(["relu", "sigmoid", "tanh"])
    optimizer = random.choice(["gd", "sgd", "adam"])

    mlflow.log_param("batch_size", batch_size)
    mlflow.log_param("learning_rate", learning_rate)
    mlflow.log_param("drop_out", drop_out)
    mlflow.log_param("kernel_size", kernel_size)
    mlflow.log_param("stopped_epoch", stopped_epoch)
    mlflow.log_param("activation", activation)
    mlflow.log_param("optimizer", optimizer)

def log_metrics():
    
    # accuracy metrics
    val_acc_start = random.uniform(0.1, 0.25)
    acc_start = random.uniform(0.1, 0.25)
    val_acc_stop = random.uniform(0.73, 0.96)
    acc_stop = random.uniform(0.75, 0.98)

    # loss metrics
    val_loss_start = random.uniform(2, 1)
    loss_start = random.uniform(2, 1)
    val_loss_stop = random.uniform(0.12, 0.52)
    loss_stop = random.uniform(0.1, 0.5)

    # set initial values
    val_acc = val_acc_start
    acc = acc_start
    val_loss = val_loss_start
    loss = loss_start
    
    for i in range(100):
        mlflow.log_metric('val_accuracy', val_acc, step=i)
        mlflow.log_metric('accuracy', acc, step=i)
        mlflow.log_metric('val_loss', val_loss, step=i)
        mlflow.log_metric('loss', loss, step=i)

        val_acc = val_acc + (val_acc_stop - val_acc) * 0.1 - random.uniform(0.0001, 0.01)
        acc = acc + (acc_stop - acc) * 0.1 - random.uniform(0.0001, 0.01)
        val_loss = val_loss - (val_loss - val_loss_stop) * 0.1 + random.uniform(0.0001, 0.01)
        loss = loss - (loss - loss_stop) * 0.1 + random.uniform(0.0001, 0.01)
        time.sleep(0.2)

    mlflow.log_metric('test_accuracy', acc_stop)
    mlflow.log_metric('test_loss', loss_stop)

def log_artifacts():
    mlflow.log_artifact("confusion_matrix.png")
    mlflow.log_artifact("feature_importance.png")
    mlflow.log_artifact("team.jpeg")
    mlflow.log_artifact("model.pt")

def create_single_run(experiment_name, nested=False):
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(nested=True) as run:
        print("Creating... Experiment: {}, Run ID: {}".format(experiment_name, run.info.run_id))
        log_tags()
        log_params()
        log_metrics()
        log_artifacts()

def create_multiple_runs(experiment_name, num_runs):
    for i in range(num_runs):
        create_single_run(experiment_name)

def create_nested_runs(experiment_name, num_children):
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run() as parent_run:
        for i in range(num_children):
            create_single_run(experiment_name, True)

def create_multiple_experiments(experiment_name, num_experiments, num_runs_per_experiment, nested_runs=False):
    print("Creating multiple experiments ...")
    for i in range(num_experiments):
        unique_name = "{}-{}".format(experiment_name, i)
        if nested_runs:
            create_nested_runs(unique_name, num_runs_per_experiment)
        else:
            create_multiple_runs(unique_name, num_runs_per_experiment)

parser = ArgumentParser(description="Mock trainer that logs ML Flow runs")
parser.add_argument("--experiment_name", type=str, default="test", help="Name of experiment. If creating multiple experiments, unique names will be generated from string")
parser.add_argument("--num_experiments", type=int, default=1, help="Number of experiments to generate")
parser.add_argument("--num_runs", type=int, default=1, help="Number of runs to generate per experiment and/or parent")
parser.add_argument("--nested_runs", type=bool, default=False, help="Whether to generated nested runs or not")
args = vars(parser.parse_args())

experiment_name = args["experiment_name"]
num_experiments = args["num_experiments"]
num_runs = args["num_runs"]
nested_runs = args["nested_runs"]

if num_experiments == 0 or num_runs == 0:
    print("Invalid arguments")

if num_experiments == 1:
    if nested_runs:
        create_nested_runs(experiment_name, num_runs)
    else:
        create_multiple_runs(experiment_name, num_runs)

if num_experiments > 1:
    if nested_runs:
        create_multiple_experiments(experiment_name, num_experiments, num_runs, True)
    else:
        create_multiple_experiments(experiment_name, num_experiments, num_runs)



