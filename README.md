# sims_longterm_tests
Test ground for long-term simulation generation

Sims cloned at `/sdf/data/rubin/shared/fbs_sims/sims_longterm_tests/`:
- Baseline simulation - copy of baseline from v4.3.1; in `baseline` folder.
- Weather simulations - copy of baseline from v4.3.1; in `weather` folder. Note:had to move what seems to be a buggy file in a subfolder `buggy`.
- Plots - vector metric Nvisits (Nvisits per filter + all filters) over time, for each simulation.

Goal is to have two kinds of simulations:
1. Chimera simuations -  a weather simulation up to date X, then baseline simulation after date X.
2. Continuing/bespoke simulations -  a weather simulation up to date X, then continue simulation using baseline configuration.

Code structure (all in `scripts` folder in this repo):
- primary script is `run.py` with various options. uses `config.yml` and is easily run by `run.sh` (see the script for how to enable various stages of the analysis).
    - `run.py` calls three helper functions, in `get_fonvtime.py`, `get_chimera.py`, and `get_bespoke.py`

As an example, results for two `cutoff_dates` (X above) are shown in `notebooks/dev-analysis.ipynb`.

Misc notes:
- bespoke sims take a while (9-14hrs), hence the need for in-queue scripts for each weather sim; see more in `run.sh`.
- everything except bespoke sims generations can happen on an interactive node - but for ease, `run.sh` includes the option to run various stages in queue as well.