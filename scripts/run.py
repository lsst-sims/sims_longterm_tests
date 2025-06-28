###############################################################################
# script to run things for all sims - e.g., the vector metrics
###############################################################################
import os
import numpy as np
import yaml
import time
from optparse import OptionParser
from get_fonvtime import get_fonvtime
from get_chimera import get_chimera
import pickle
###############################################################################
parser = OptionParser()
parser.add_option('--config', dest='config_path',
                  help='path to (yml) config file with outdir path, etc.'
                  )
parser.add_option('--fonvbase', dest='fonv_base',
                  action='store_true', default=False,
                  help='flag to run fonv vector metric on baseline+weather ' +
                  'sims; saves data.'
                  )
parser.add_option('--chimera', dest='chimera',
                  action='store_true', default=False,
                  help='flag to generate chimera sims + run metrics on them.'
                  )
parser.add_option('--cutoff', dest='cutoff_date',
                  help='date for cutoff; YYYY-MM-DD format.'
                  )
# ---------------------------------------------------------
start_time = time.time()
options, _ = parser.parse_args()
# read inputs
config_path = options.config_path
fonv_base = options.fonv_base
chimera = options.chimera
cutoff_date = options.cutoff_date #'2026-03-01'
if chimera and cutoff_date is None:
    raise ValueError(f'## must specify cutoff_date when using chimera flag.')

# load the config
with open(config_path, 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
# pull out things we need
basepath = config['basepath']
outdir = config['outdir']
nside = config['nside']
tag_to_look_for = config['tag_to_look_for']
# set up time array for the vector metric
timepts = config['timepts']
time_points = np.arange(timepts[0], timepts[1], timepts[2])
del timepts

# outdir for metrics
outdir_metrics= f'{outdir}/metrics/'
os.makedirs(outdir_metrics, exist_ok=True)
# now run
# ---------------------------------------------------------------
if fonv_base:
    # ---------------------------------------------------------------
    time0 = time.time()
    print(f'## running vector metric for baseline sims ...')
    save_data = True
    fonvs_time_all, fonvs_time_per_filter = {}, {}
    # outdir for the interim outputs
    subdir = f'{outdir_metrics}/fonvs_base'
    os.makedirs(subdir, exist_ok=True)
    # ---------------------------------------------------------------
    # loop over the two folders
    for cat in ['baseline', 'weather']:
        dbpath = f'{basepath}/{cat}'
        for opsim_fname in [f for f in os.listdir(dbpath) if \
                                            f.endswith(tag_to_look_for)]:
            print(f'## working with {opsim_fname}')
            db_tag = opsim_fname.split(tag_to_look_for)[0]
            opsim_path = f'{dbpath}/{opsim_fname}'

            # ---------------------------------------------------------------
            # median nvisits over survey area as a function of time
            # all filters
            constraint = "scheduler_note not like '%DD%'"
            fonvs_time_all[db_tag] = get_fonvtime(constraint=constraint,
                                                  nside=nside,
                                                  time_points=time_points,
                                                  opsim_path=opsim_path,
                                                  outdir=subdir,
                                                  save_data=save_data,
                                                  output_tag=f'{db_tag}_allfilts'
                                                  )
            # ---------------------------------------------------------------
            # now by filter
            for filt in 'ugrizy':
                constraint = f"scheduler_note not like '%DD%' and filter='{filt}'"
                if filt not in fonvs_time_per_filter:
                    fonvs_time_per_filter[filt] = {}
                fonvs_time_per_filter[filt][db_tag] = get_fonvtime(constraint=constraint,
                                                                   nside=nside,
                                                                   time_points=time_points,
                                                                   opsim_path=opsim_path,
                                                                   outdir=subdir,
                                                                   save_data=save_data,
                                                                   output_tag=f'{db_tag}_{filt}'
                                                                   )
    # ---------------------------------------------------------------
    # now save
    fname = 'fonvs_vector_base.pickle'
    pickle.dump({'fonvs_time_all': fonvs_time_all,
                 'fonvs_time_per_filter': fonvs_time_per_filter
                 },
                 open(f'{outdir_metrics}/{fname}', 'wb')
                 )
    print(f'## fonvs dicts saved in {fname}.')
    print(f'## time taken: {(time.time() - time0)/60:.2f} (min)')
    # ---------------------------------------------------------------

# ---------------------------------------------------------------
if chimera:
    # ---------------------------------------------------------------
    time0 = time.time()
    print(f'## working on chimera sims ...')
    # outdir for chimera sims
    outdir_chimera = f'{outdir}/chimera/'
    os.makedirs(outdir_chimera, exist_ok=True)
    # outdir for the interim outputs
    subdir = f'{outdir_metrics}/fonvs_chimera/'
    os.makedirs(subdir, exist_ok=True)
    # set up
    chimera_fonvs_time_all, chimera_fonvs_time_per_filter = {}, {}
    save_data = True
    # set up the baseline path
    baseline_path = [f for f in os.listdir(f'{basepath}/baseline/') if \
                                            f.endswith(tag_to_look_for)]
    if len(baseline_path) != 1:
        raise ValueError(f'## expecting 1 baseline; got {baseline_path}')
    else:
        baseline_path = f'{basepath}/baseline/{baseline_path[0]}'

    # loop over the weather sims
    for cat in ['weather']:
        dbpath = f'{basepath}/{cat}'
        for opsim_fname in [f for f in os.listdir(dbpath) if f.endswith(tag_to_look_for)]:
            print(f'## working with {opsim_fname}')
            db_tag = opsim_fname.split(tag_to_look_for)[0]
            opsim_path = f'{dbpath}/{opsim_fname}'
            # ---------------------------------------------------------------
            # generate the chimera sim
            opsim_path = get_chimera(baseline_path=baseline_path,
                                     sim_to_cut_path=opsim_path,
                                     cutoff_date=cutoff_date,
                                     cutoff_date_format='isot',
                                     outdir=outdir_chimera
                                     )
            # now run things for the sim
            db_tag = f'chimera_cutoff{cutoff_date}_{db_tag}'
            print(opsim_path)
            # ---------------------------------------------------------------
            # nvisits as a function of time
            # all filters
            constraint = "scheduler_note not like '%DD%'"
            chimera_fonvs_time_all[db_tag] = get_fonvtime(constraint=constraint,
                                                          nside=nside,
                                                          time_points=time_points,
                                                          opsim_path=opsim_path,
                                                          outdir=subdir,
                                                          save_data=save_data,
                                                          output_tag=f'{db_tag}_allfilts'
                                                          )
            # ---------------------------------------------------------------
            # now by filter
            for filt in 'ugrizy':
                constraint = f"scheduler_note not like '%DD%' and filter='{filt}'"
                if filt not in chimera_fonvs_time_per_filter:
                    chimera_fonvs_time_per_filter[filt] = {}
                chimera_fonvs_time_per_filter[filt][db_tag] = get_fonvtime(constraint=constraint,
                                                                           nside=nside,
                                                                           time_points=time_points,
                                                                           opsim_path=opsim_path,
                                                                           outdir=subdir,
                                                                           save_data=save_data,
                                                                           output_tag=f'{db_tag}_{filt}'
                                                                           )
    #  ---------------------------------------------------------------
    # now save
    fname = f'fnovs_vector_chimera_cutoff{cutoff_date}.pickle'
    pickle.dump({'chimera_fnovs_time_all': chimera_fnovs_time_all,
                 'chimera_fnovs_time_per_filter': chimera_fnovs_time_per_filter
                 },
                 open(f'{outdir_metrics}/{fname}', 'wb')
                 )
    print(f'## chimera fnovs dicts saved in {fname}.')
    print(f'## time taken: {(time.time() - time0)/60:.2f} (min)')
    # ---------------------------------------------------------------

print(f'## overall time taken: {(time.time() - start_time)/60:.2f} (min)')