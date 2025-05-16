###############################################################################
# script to run things for all sims - e.g., the vector metrics
###############################################################################
import os
import numpy as np
import yaml
import time
from optparse import OptionParser
from get_fnovtime import get_fnovtime
import pickle
###############################################################################
parser = OptionParser()
parser.add_option('--config', dest='config_path',
                  help='path to (yml) config file with outdir path, etc.'
                  )
parser.add_option('--fnovbase', dest='fnov_base',
                  action='store_true', default=False,
                  help='flag to run fnov vector metric on baseline+weather ' +
                  'sims; saves data.'
                  )
# ---------------------------------------------------------
start_time = time.time()
options, _ = parser.parse_args()
# read inputs
config_path = options.config_path
fnov_base = options.fnov_base
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

# now run
# ---------------------------------------------------------------
if fnov_base:
    # ---------------------------------------------------------------
    time0 = time.time()
    print(f'## running vector metric for baseline sims ...')
    save_data = True
    fnovs_time_all, fnovs_time_per_filter = {}, {}
    # outdir for the interim outputs
    subdir = f'{outdir}/fnovs_base'
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
            fnovs_time_all[db_tag] = get_fnovtime(constraint=constraint,
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
                if filt not in fnovs_time_per_filter:
                    fnovs_time_per_filter[filt] = {}
                fnovs_time_per_filter[filt][db_tag] = get_fnovtime(constraint=constraint,
                                                                   nside=nside,
                                                                   time_points=time_points,
                                                                   opsim_path=opsim_path,
                                                                   outdir=subdir,
                                                                   save_data=save_data,
                                                                   output_tag=f'{db_tag}_{filt}'
                                                                   )
    # ---------------------------------------------------------------
    # now save
    fname = 'fnovs_vector_base.pickle'
    pickle.dump({'fnovs_time_all': fnovs_time_all,
                 'fnovs_time_per_filter': fnovs_time_per_filter
                 },
                 open(f'{outdir}/{fname}', 'wb')
                 )
    print(f'## fnovs dicts saved in {fname}.')
    print(f'## time taken: {(time.time() - time0)/60:.2f} (min)')
    # ---------------------------------------------------------------

print(f'## overall time taken: {(time.time() - start_time)/60:.2f} (min)')