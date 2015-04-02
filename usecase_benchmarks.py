import os
import sys
import time
import argparse
import logging
import threading
import contextlib
from StringIO import StringIO
from functools import partial

from elapsed_time import elapsed_time
from dvid_url_fields import DvidUrlFields

# roi_utils was copied from lazyflow/roi.py
from roi_utils import getIntersectingRois

log_formatter = logging.Formatter('%(levelname)s %(name)s %(message)s')
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(log_formatter)

CLUSTER_ACCESS_SERVER = 'login2'
CLUSTER_PYTHON_EXE = '/groups/flyem/proj/cluster/miniconda/envs/stuart/bin/python'
CLUSTER_NODE_TASK_SCRIPT = '/groups/flyem/proj/cluster/pydvid-benchmarks/usecase_benchmarks.py'

def main():
    # First arg tells us whether or not this is the master process.
    task_type = sys.argv[1]
    if task_type == "master":
        sys.exit( master_main() )
    elif task_type == "node":
        sys.exit( node_main() )
    else:
        sys.stderr.write("Usage: {} [master|node] <args...>\n")
        sys.exit(1)

def master_main():
    """
    The main entry point for the master process.
    Decides how to split the volume and then uses 'qsub' to launch the node tasks for each subvolume.
    """
    import fabric.api as fab
    fab.env.host_string = CLUSTER_ACCESS_SERVER

    logger = logging.getLogger("master_main")
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("master", choices=["master"])
    parser.add_argument("dvid_volume_url", type=DvidUrlFields)
    parser.add_argument("total_roi", type=eval)
    parser.add_argument("node_blockshape", type=eval)
    parser.add_argument("request_blockshape", type=eval)
    parser.add_argument("threads_per_node", type=int)
    parser.add_argument("--cwd", default=os.getcwd())
    parsed_args = parser.parse_args()

    # Error checks
    assert len(parsed_args.total_roi) == 2
    assert (    len(parsed_args.total_roi[0]) 
             == len(parsed_args.total_roi[1]) 
             == len(parsed_args.node_blockshape) 
             == len(parsed_args.request_blockshape) )

    def launch_node( node_roi, task_name ):
        task_args = "node '{}' {} '{}' '{}' {}"\
                    .format( task_name, 
                             parsed_args.dvid_volume_url,
                             node_roi, 
                             parsed_args.request_blockshape, 
                             parsed_args.threads_per_node )

        qsub_cmd = 'qsub -pe batch {num_cpus} -l short=true -N {task_name} -o {task_output_file} -j y -b y -cwd -V "{python_exe} {node_task_script} {task_args}"'\
                   .format( num_cpus=parsed_args.threads_per_node,
                            task_name=task_name,
                            task_output_file=task_name + '.output',
                            python_exe=CLUSTER_PYTHON_EXE,
                            node_task_script=CLUSTER_NODE_TASK_SCRIPT,
                            task_args=task_args )

        # Execute on remote server
        with fab.cd(parsed_args.cwd):
            logger.info(qsub_cmd)
            cmd_result = fab.run( qsub_cmd )
            if len(cmd_result.stderr) > 0:
                raise Exception("Failed to launch node task.  Output was: {}".format( cmd_result.stderr ))
            assert cmd_result.startswith("Your job")
            assert cmd_result.endswith("has been submitted")
            job_id_string = cmd_result.split()[2]
            return job_id_string

    # Remove old output files first.
    with fab.cd(parsed_args.cwd):
        rm_cmd = "rm -f *.output"
        logger.info(rm_cmd)
        fab.run( rm_cmd )
    
    node_rois = getIntersectingRois( parsed_args.total_roi[1],
                                     parsed_args.node_blockshape,
                                     parsed_args.total_roi )
    job_ids = []
    for index, (start, stop) in enumerate(node_rois):
        job_id_str = launch_node( (list(start), list(stop)), "J{:04}".format(index) )
        job_ids.append(job_id_str)

    logger.info( "Waiting for {} jobs: {}".format( len(job_ids), job_ids ) )
    wait_for_jobs(job_ids)

    logger.info( "FINISHED." )

def wait_for_jobs( job_ids, hide_output=True, poll_interval=2.0 ):
    import fabric.api as fab
    fab.env.host_string = CLUSTER_ACCESS_SERVER
    while job_ids:
        if hide_output:
            with fab.hide('running'):
                qstat_output = fab.run('qstat', stdout=StringIO())
        else:
            qstat_output = fab.run('qstat')

        if qstat_output.failed:
            raise Exception("Error executing qstat.  Output was: \n" + qstat_output)

        for job_id in list(job_ids):
            if job_id not in qstat_output:
                job_ids.remove(job_id)

        if job_ids:
            time.sleep(poll_interval)

def node_main():
    """
    The main entry point for a node task (launched via qsub by the master process).
    Requests the specified volume from DVID, and logs the elapsed time.
    """
    from pydvid.voxels import VoxelsAccessor
    from pydvid.dvid_connection import DvidConnection

    parser = argparse.ArgumentParser()
    parser.add_argument("node", choices=["node"])
    parser.add_argument("task_name", type=str)
    parser.add_argument("dvid_volume_url", type=DvidUrlFields)
    parser.add_argument("node_roi", type=eval)
    parser.add_argument("request_blockshape", type=eval)
    parser.add_argument("num_threads", type=int)
    parsed_args = parser.parse_args()

    logger = logging.getLogger("node." + parsed_args.task_name)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    connection = DvidConnection(parsed_args.dvid_volume_url.hostname)
    with contextlib.closing(connection):
        dvid_accessor = VoxelsAccessor( connection, 
                                        parsed_args.dvid_volume_url.uuid, 
                                        parsed_args.dvid_volume_url.dataname,
                                        parsed_args.dvid_volume_url.query_args )

        block_rois = getIntersectingRois( parsed_args.node_roi[1],
                                          parsed_args.request_blockshape,
                                          parsed_args.node_roi )

        def request_block( block_roi ):
            node_data = dvid_accessor.get_ndarray( *block_roi )

        threads = []
        for block_roi in block_rois:
            t = threading.Thread(target=partial(request_block, block_roi))
            threads.append( t )
        
        for t in threads:
            t.start()
        
        with elapsed_time(logger) as elapsed:
            for t in threads:
                t.join()

    logger.info("DONE.")
    
if __name__ == "__main__":
    # DEBUG ARGS
    if len(sys.argv) == 1:
        DEBUG_MODE = "master"
        if DEBUG_MODE == "master":
            sys.argv += [ "master",
                          "http://emdata1:8000/api/repo/1c6ebbd7870511e4b3dc90b11c576b54/grayscale",
                          "[(0,100,1000,1000), (1,200,2000,2000)]",
                          "(1,10,1000,1000)",
                          "(1,10,250,250)",
                          "4",
                          "--cwd=/groups/flyem/home/bergs/dvid_testing" ]
            
        elif DEBUG_MODE == 'node':
            sys.argv.append("node")
            assert False
            
    main()
