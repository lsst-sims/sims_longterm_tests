#!/bin/bash
# -----------------------------------------------------------------------------
# this script contains options to run various parts of the ground work for
# longer term sims. change repopath, cutoff_date as needed, alongside the
# various options.
#
# note also that the slurm scripts include
#   source ~/.bashrc
#   conda activate ss
# that load up in the environment with rubin_sim, rubin_scheduler; update as
# needed.
# -----------------------------------------------------------------------------
repopath=/sdf/home/h/humna/u/repos/sims_longterm_tests/
mkdir -p ${repopath}'/slurm_out'
mkdir -p ${repopath}'/slurm_scripts'
cd ${repopath}'/slurm_scripts'

cutoff_date=2026-03-01
# cutoff_date=2028-09-01

config=${repopath}/scripts/config.yml
outpath=${repopath}/slurm_out

# options
fonv_base_queue=0   # set to 1 to run fonv metric on basleine
chimera_queue=0     # set to 1 to generate chimera sims and run fonv metric on them
bespoke_sims_queue=0    # set to 1 to generate bespoke sims
bespoke_metrics_queue=1 # set to 1 to run fonv metric on bespoke sims; must have bespoke sims already
various_interactive=0   # set to 1 to run things in interactive node

# -----------------------------------------------------------------------------
# run fnov on baseline - in queue
# -----------------------------------------------------------------------------
if [[ $fonv_base_queue == 1 ]];
then
    jobname='fbase'
    fname='fonv_base'

    cat > ${fname}.sl << EOF
#!/bin/bash
#SBATCH --account=rubin:commissioning
#SBATCH --partition=milano
#SBATCH --job-name=${jobname}
#SBATCH --output=${outpath}/${fname}_%j.out
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=20g
#SBATCH --time=00:30:00

source ~/.bashrc
conda activate ss

export OMP_NUM_THREAD=1
export USE_SIMPLE_THREADED_LEVEL3=1

echo '## runnig fonv metric on baseline ..'
python ${repopath}/scripts/run.py --config=${config} --fonvbase
EOF
    sbatch ${fname}.sl
    echo submitted ${fname}.sl
fi

# -----------------------------------------------------------------------------
# chimera sims generation + fnov metric - queue option
# -----------------------------------------------------------------------------
if [[ $chimera_queue == 1 ]];
then
    jobname='ch'
    fname='chimera_cutoff'${cutoff_date}
    cat > ${fname}.sl << EOF
#!/bin/bash
#SBATCH --account=rubin:commissioning
#SBATCH --partition=milano
#SBATCH --job-name=${jobname}
#SBATCH --output=${outpath}/${fname}_%j.out
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=20g
#SBATCH --time=01:00:00

source ~/.bashrc
conda activate ss

export OMP_NUM_THREAD=1
export USE_SIMPLE_THREADED_LEVEL3=1

echo '## creating  chimera sims and running fonv metric on them ..'
python ${repopath}/scripts/run.py --config=${config} \
        --chimera --cutoff=${cutoff_date}

EOF
    sbatch ${fname}.sl
    echo submitted ${fname}.sl
fi

# -----------------------------------------------------------------------------
# bespoke sims generation - has to be in queue
# -----------------------------------------------------------------------------
if [[ $bespoke_sims_queue == 1 ]];
then
    counter=0
    dbs_path='/sdf/data/rubin/shared/fbs_sims/sims_longterm_tests/weather/'
    for dbpath in $(find ${dbs_path} -name '*_10yrs.db')
    do
        dbname=$(awk -F ${dbs_path} '{print $2}' <<< "$dbpath")
        dbname=$(awk -F '.db' '{print $1}' <<< "$dbname")
        echo '## running things for '${dbname}
        jobname='bsp'${counter}
        fname='bespoke_sim_'${dbname}'_cutoff'${cutoff_date}

        cat > ${fname}.sl << EOF
#!/bin/bash
#SBATCH --account=rubin:commissioning
#SBATCH --partition=milano
#SBATCH --job-name=${jobname}
#SBATCH --output=${outpath}/${fname}_%j.out
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=30g
#SBATCH --time=15:00:00

source ~/.bashrc
conda activate ss

export OMP_NUM_THREAD=1
export USE_SIMPLE_THREADED_LEVEL3=1

python ${repopath}/scripts/run.py --config=${config} \
            --bespoke-sim-only --bespoke-opsim-fname=${dbpath} \
            --cutoff=${cutoff_date}

EOF
        sbatch ${fname}.sl
        echo submitted ${fname}.sl

        ((counter++))
    done
fi


# -----------------------------------------------------------------------------
# fnov metric on bespoke sims - queue option
# -----------------------------------------------------------------------------
if [[ $bespoke_metrics_queue == 1 ]];
then
    jobname='bspmets'
    fname='bespoke_metrics_cutoff'${cutoff_date}
    cat > ${fname}.sl << EOF
#!/bin/bash
#SBATCH --account=rubin:commissioning
#SBATCH --partition=milano
#SBATCH --job-name=${jobname}
#SBATCH --output=${outpath}/${fname}_%j.out
#SBATCH --nodes=1                       # Number of nodes
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=20g
#SBATCH --time=01:00:00

source ~/.bashrc
conda activate ss

export OMP_NUM_THREAD=1
export USE_SIMPLE_THREADED_LEVEL3=1

echo '## running fonv metric on bespoke sims ..'
python ${repopath}/scripts/run.py --config=${config} \
        --bespoke-metrics --cutoff=${cutoff_date}

EOF
    sbatch ${fname}.sl
    echo submitted ${fname}.sl
fi

# -----------------------------------------------------------------------------
# option to run on interactive node; must already be on it and have the right
# environment loaded
# -----------------------------------------------------------------------------
if [[ $various_interactive == 1 ]];
then
    # run fonv metric on baseline
    echo '## runnig fonv metric on baseline ..'
    python ${repopath}/scripts/run.py --config=${config} --fonvbase --cutoff=${cutoff_date} > ${outpath}/fonvbase.out

    # generate chimera sims - and run metrics on them
    echo '## generating chimera sims and running fonv metric on them ..'
    python ${repopath}/scripts/run.py --config=${config} --chimera --cutoff=${cutoff_date} > ${outpath}/chimera.out

    # run metrics on bespoke sims
    echo '## running fonv metric on bespoke sims ..'
    python ${repopath}/scripts/run.py --config=${config} --bespoke-metrics --cutoff=${cutoff_date} > ${outpath}/bespoke.out

fi