
from rubin_sim import maf
import healpy as hp
import numpy as np
import os

__all__ = ['FONvTime', 'get_fnovtime']

###############################################################################
# fonov vector metric; from
# https://github.com/yoachim/25_scratch/blob/main/vector_metrics/fonv_time.ipynb
class FONvTime(maf.metrics.BaseMetric):
    """Given a vector metric with number of observations over time, convert to
    FONv over time.
    """
    # ---------------------------------------------------------
    def __init__(self, asky=18000.0, stat=np.median, **kwargs):
        super().__init__(**kwargs)
        self.asky = asky
        self.stat = stat
        # This should get full vector metric passed
        # with masked values set to zero
        self.mask_val = 0 

    # ---------------------------------------------------------
    def run(self, data_slice, slice_point=None):
        # Should be able to add a check on data_slice dim,
        # then just promote it to an (N,1) array if
        # it's a single map. 
        n_pix_heal = data_slice["metricdata"][:,0].size
        nside = hp.npix2nside(n_pix_heal)
        pix_area = hp.nside2pixarea(nside, degrees=True)
        n_pix_needed = int(np.ceil(self.asky/pix_area))
        # sort by value
        data = data_slice["metricdata"].copy()
        data.sort(axis=0)
        # Crop down to the desired sky area
        data = data[n_pix_heal-n_pix_needed:, :]
        result = self.stat(data, axis=0)
        return result

###############################################################################
def get_fnovtime(constraint, nside, time_points, opsim_path, outdir,
                 save_data=False, output_tag=None
                 ):
    """
    required inputs
    ---------------
    * constraint: str: sql constraint for visits
    * nside: int: healpix resolution parameter
    * time_points: arr: time points at which to get fnov.
    * opsim_path: str: path to the opsim database
    * outdir: str: output directory

    optional inputs
    ---------------
    * save_data: bool: set to True to save the metric value.
                       default: False
    * output_tag: str: tag to put in the output file; signifies combo of db,
                       constraint, etc. default: None

    returns
    -------
    * array: fonv vector metric values

    """
    # ---------------------------------------------------------
    if save_data and output_tag is None:
        raise ValueError(f'## must specificy output_tag if save_data=True')
    
    # set up the output filename
    fname = f'{outdir}/fonv_{output_tag}_nside{nside}.npz'

    # lets also make a subdir for maf outputs
    subdir = f'{outdir}/maf/'
    os.makedirs(subdir, exist_ok=True)

    # look for the output file
    if os.path.exists(fname):
        print(f'## reading data from {fname} ...\n')
        return np.load(fname)['fnovtime']
    else:
        # run the metric
        slicer = maf.slicers.HealpixSlicer(nside=nside, use_cache=False)
        metric = maf.metrics.AccumulateCountMetric(bins=time_points, col='visitExposureTime')
        summary_metrics = [FONvTime()]
        
        bundle = maf.MetricBundle(metric, slicer, constraint, summary_metrics=summary_metrics)
        bundle_grp = maf.MetricBundleGroup([bundle], opsim_path, out_dir=subdir)
        bundle_grp.run_all()

        if save_data:
            print(f'## saved data as {fname}\n')
            np.savez_compressed(fname, fnovtime=bundle.summary_values['FONvTime'])

        return bundle.summary_values['FONvTime']
   