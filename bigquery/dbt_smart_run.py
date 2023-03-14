# To execute, run this file in your terminal, with the name(s) of the target models you'd like updated.
# Example usage: 
#   $ python3 dbt_smart_run.py -targets your_first_target_model
#   $ python3 dbt_smart_run.py -targets your_first_target_model your_second_target_model

# This script requires python package(s).  Install dependencies by running the following:
#   $ pip3 install -r ../requirements.txt

import os, os.path, sys, git, re, subprocess, argparse

parser = argparse.ArgumentParser(prog = 'dbt_smart_run', description = 'syntax: python3 dbt_smart_run.py -targets model_A model_B')
parser.add_argument("-targets", nargs='+', help='define one or many target models you would like to run')
parser.add_argument("--skip_copy", action="store_true", help='skips the copy execution for identified models')
args = parser.parse_args()

def generate_changed_models():
  g = git.Git(os.path.expanduser("../../analytics"))
  get_committed_files = g.execute(["git", "diff", "--name-only", "--diff-filter=d", "main..."])
  get_uncommitted_changed_files = g.execute(["git", "diff", "--name-only", "--diff-filter=d"])
  changed_files = get_committed_files + '\n' + get_uncommitted_changed_files
  changed_file_list_with_duplicates = changed_files.replace("\t", "\n").split("\n")
  changed_file_list = list(set(changed_file_list_with_duplicates))

  changed_models = []
  for model in changed_file_list:
    if(os.path.isfile('../' + model)): #only care about files, not folders
      filepath, file_extension = os.path.splitext(model)
      filepath_list = filepath.split('/')
      if file_extension == '.sql' and filepath_list[0] == 'dbt_ft_prod' and filepath_list[1] == 'models':
        filename = filepath_list[-1]
        changed_models.append(filename)
  if not changed_models:
    print('No models have been changed. Have you saved all of your changes?')
    print('---Note, only .sql files in the /models directory are considered.')
  else:
    print(f'The following models have been logged as models that have been changed: {changed_models}')
  return changed_models
  
def run_dbt_smart_run():
  targets = args.targets
  if not targets:
    print('Data quality rejection: No target models have been specified.')
    return
  
  changed_models = generate_changed_models()

  if args.skip_copy:
    skip_copy_flag = ", 'skip_copy':['true']"
  else:
    skip_copy_flag = ""
  
  command = "dbt run-operation dbt_smart_run --args \"{'targets':"+ str(targets) + ", 'models_updated':" + str(changed_models) + skip_copy_flag + "}\""
  os.system(command)
  response = subprocess.check_output(command, shell=True).decode("utf-8")

  if re.search("(?<=\$ )(.*)(?=\\n)", response): 
    dbt_run_command = re.search("(?<=\$ )(.*)(?=\\n)", response).group()
    os.system(dbt_run_command)
  else:
    return

run_dbt_smart_run()