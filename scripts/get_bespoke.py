import pandas as pd
import sqlite3
import os
import numpy as np
from types import SimpleNamespace
from astropy.time import Time
import sys
import importlib
from rubin_scheduler.scheduler.utils import SchemaConverter, restore_scheduler
from rubin_scheduler.scheduler.schedulers import SimpleBandSched
from rubin_scheduler.scheduler.model_observatory import ModelObservatory
from rubin_scheduler.scheduler import sim_runner

__all__ = ['get_bespoke']

###############################################################################
def get_bespoke(baseline_py_path, sim_to_cut_path, cutoff_date, cutoff_date_format,
                outdir, scheduler_args, illum_limit=40, exists_only=False
                ):
    """
    generate a new sqlite database, taking observations from database at
    sim_to_cut_path <= cutoff date, and then running the scheduler from there
    using baseline config.

    required inputs
    ---------------
    * baseline_py_path: str: path to the .py file that generated the baseline
                             opsim database (or the one to use to generate
                             visits past the cutoff date)
    * sim_to_cut_path: str: path to the database for which to keep observations
                            up to the cutoff date
    * cutoff_date: str: cutoff date, e.g. in mjd or isot format
    * cutoff_date_format: str: format for cutoff_date, e.g. 'mjd', 'isot'
    * outdir: str: output directory
    * scheduler_args: str: dictionary with arguments to pass to gen_scheduler


    optional inputs
    ---------------
    * illum_limit: float: param needed for SimpleBandSched; unclear what it
                          means but is specified as part of the baseline config.
                          default: 40
    * exists_only: bool: set to True only look for a simulation and return path
                         if it exists already. otherwise return None.
                         default: False

    returns
    -------
    * path to the new database

    """
    # ---------------------------------------------------------
    cutoff_mjd = Time(f'{cutoff_date}T12:00:00', format=cutoff_date_format).mjd
    # fname to save
    fname = f"bespoke_{sim_to_cut_path.split('/')[-1].split('.db')[0]}_"
    fname += f"{baseline_py_path.split('/')[-1].split('.py')[0]}-sched_cutoff{cutoff_mjd}.db"

    # lets see if the db exists already
    if os.path.exists(f'{outdir}/{fname}'):
        print(f'## bespoke sim exists already: {outdir}/{fname}\n')
        return f'{outdir}/{fname}'
    if exists_only:
        print(f'## bespoke sim DOESNT doesnt exist: {outdir}/{fname}\n')
        return None
    # first get the visits upto the cutoff date
    conn = sqlite3.connect(sim_to_cut_path)
    query = f"select * from observations where observationStartMJD <= {cutoff_mjd}"
    observations = pd.read_sql(query, conn)
    conn.close()
    # ok so we have the observations up until the cutoff
    # add bogus columns ..
    cols_to_add = ['note', 'cloud_extinction']
    for col in cols_to_add:
        observations[col] = np.zeros_like(observations['observationId'])
    # need to convert opsim column names to those needed for scheduler
    converter = SchemaConverter()
    obs_in = converter.opsimdf2obs(observations)
    # other things needed to restore scheduler
    # lets pull in the functions used in generating the baseline
    sys.path.append(os.path.dirname(baseline_py_path))
    baseline_py = importlib.import_module(
                        baseline_py_path.split('/')[-1].split('.py')[0]
                        )
    gen_scheduler = getattr(baseline_py, 'gen_scheduler')
    # scheduler args
    args = SimpleNamespace(**scheduler_args)
    args.survey_length = args.survey_length - observations['night'].max() + 1
    print(f'## working on simulating {args.survey_length} nights.')
    # we only want the scheduler so lets just update the arg for that
    args.setup_only = True
    args.verbose = True
    print(f'args = {args}')
    # now create the scheduler
    scheduler = gen_scheduler(args)
    # also create the band scheduler
    band_sched = SimpleBandSched(illum_limit=illum_limit)
    # now the model observatory
    observatory = ModelObservatory(nside=scheduler.nside,
                                   mjd_start=observations['observationStartMJD'].min(),
                                   sim_to_o=None)
    # now restore rescheduler to the obsID we cut at
    scheduler, observatory = restore_scheduler(observation_id=observations['observationId'].max(),
                                               scheduler=scheduler,
                                               observatory=observatory,
                                               in_obs=obs_in,
                                               band_sched=band_sched, fast=True)
    # run the sims
    _, _, observations_new = sim_runner(observatory, scheduler,
                                        sim_duration=args.survey_length,
                                        filename=None,
                                        delete_past=True,
                                        n_visit_limit=None,
                                        verbose=True,
                                        extra_info=None,
                                        band_scheduler=band_sched,
                                        event_table=None,
                                        snapshot_dir=None,
                                        record_rewards=False
                                        )
    # now concatenate
    conn = sqlite3.connect(f'{outdir}/{fname}')
    observations = observations.drop(columns=cols_to_add, errors='ignore')
    pd.concat([observations,
               converter.obs2opsim(observations_new)
               ], ignore_index=True).to_sql('observations', conn,
                                            index=False, if_exists='replace')
    conn.close()

    return f'{outdir}/{fname}'