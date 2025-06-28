import pandas as pd
import sqlite3
import os
from astropy.time import Time

__all__ = ['get_chimera']

###############################################################################
def get_chimera(baseline_path, sim_to_cut_path, cutoff_date, cutoff_date_format,
                outdir
                ):
    """
    generate a new sqlite database, taking observations from database at
    sim_to_cut_path <= cutoff date, and appending the baseline observations
    after the cutoff.

    required inputs
    ---------------
    * baseline_path: str: path to the baseline database (or the one to append
                          past the cutoff date)
    * sim_to_cut_path: str: path to the database for which to keep observations
                            up to the cutoff date
    * cutoff_date: str: cutoff date, e.g. in mjd or isot format
    * cutoff_date_format: str: format for cutoff_date, e.g. 'mjd', 'isot'
    * outdir: str: output directory

    returns
    -------
    * path to the new database

    """
    # ---------------------------------------------------------
    cutoff_mjd = Time(f'{cutoff_date}T12:00:00', format=cutoff_date_format).mjd
    # fname to save
    fname = f"chimera_{sim_to_cut_path.split('/')[-1].split('.db')[0]}_"
    fname += f"{baseline_path.split('/')[-1].split('.db')[0]}_cutoff{cutoff_mjd}.db"

    # lets see if the db exists already
    if os.path.exists(f'{outdir}/{fname}'):
        print(f'## chimera sim exists already: {outdir}/{fname}\n')
        return f'{outdir}/{fname}'

    # first get the visits upto the cutoff date
    conn = sqlite3.connect(sim_to_cut_path)
    query = f"select * from observations where observationStartMJD <= {cutoff_mjd}"
    df1 = pd.read_sql(query, conn)
    conn.close()

    # now after cutoff
    conn = sqlite3.connect(baseline_path)
    query = f"select * from observations where observationStartMJD > {cutoff_mjd}"
    df2 = pd.read_sql(query, conn)
    conn.close()

    # now concatenate
    conn = sqlite3.connect(f'{outdir}/{fname}')
    pd.concat([df1, df2], ignore_index=True).to_sql('observations', conn,
                                                    index=False,
                                                    if_exists='replace'
                                                    )
    conn.close()

    return f'{outdir}/{fname}'