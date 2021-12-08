import os
import pandas as pd
from glob import glob

def get_local_samples(repo):
    """Gather sample names from local paths"""

    file_paths = glob(os.path.join(repo.location, "*.raw"))
    input_files = [os.path.split(p)[-1] for p in file_paths]
    sample_names = [os.path.splitext(f)[0] for f in input_files]

    sample_df = pd.DataFrame({"dataset" : [repo.alias]*len(file_paths),
                              "sample_name" : sample_names,
                              "input_location" : [repo.location]*len(file_paths),
                              "input_file" : input_files})
    file_select = sample_df.input_file.str.contains(repo.regex, regex=True)

    return sample_df[file_select]

def get_remote_samples(repo):
    """Use ppx to gather samples from remote repositories"""

    raise NotImplementedError("Remote files not implemented")

def get_samples(repo):
    """Determine whether to look locally or in a remote repository for samples"""

    if os.path.isdir(repo.location):
      print("Location for {} is local path,"
            "gathering samples now.".format(repo.alias))
      return get_local_samples(repo)
    else:
      print("Location for {} is not a local path,"
            "gathering samples via ppx.".format(repo.alias))
      return get_remote_samples(repo)

def get_output(sample):
    """Determine output files using the configuration settings"""

    assert config["conversion"]["format"] in ["mzML", "mgf"]
    outputs = pd.Series({"output_location" : os.path.join(config["output"], "samples", sample.dataset),
                         "output_file" : "{}.{}".format(sample.sample_name, config["conversion"]["format"])}) 

    return outputs

def get_sample_table(repo_table_path):
    """Gather input from local and remote resources and generate outputs"""

    repos = pd.read_table(repo_table_path, dtype=str)
    sample_df = pd.concat([get_samples(row) for ind, row in repos.iterrows()])
    sample_df = pd.concat([sample_df, sample_df.apply(get_output, axis=1)],
                          axis=1)

    return sample_df

def get_final_output(sample_df):
    """Paste together outputs"""

    return sample_df.apply(lambda row: os.path.join(row.output_location, row.output_file),
                           axis=1).values
