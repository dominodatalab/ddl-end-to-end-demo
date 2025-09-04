# Mock ML Flow Run Data

Generate somewhat realistic ML Flow data without actually training a model.

To generate mock ML Flow data, execute `python mock-train.py` with the following optional parameters:

- `experiment_name`: The default is **test**. Name of experiment. If creating multiple experiments, unique names will be generated from string.
- `num_experiments`: The default is **1**. Number of experiments to generate.
- `num_runs`: The default is **1**. Number of runs to generate per experiment and/or parent".
- `nested_runs`: The default is **False**. Whether to generated nested runs or not.


## Example Usage

To generate a single run:

```
python mock-train.py
```

To customize the experiment name:

```
python mock-train.py --experiment_name customExperiment
```

To generate multiple runs within an experiment:

```
python mock-train.py --num_runs 4
```

To generate nested runs within an experiment:

```
python mock-train.py --num_runs 4 --nested_runs True
```

To generate multiple experiments with multiple runs:

```
python mock-train.py --num_experiments 2 --num_runs 4
```

To generate multiple experiments with nested runs:

```
python mock-train.py --num_experiments 2 --num_runs 4 --nested_runs True
```

