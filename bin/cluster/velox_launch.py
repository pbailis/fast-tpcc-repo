import argparse
from time import sleep
from datetime import datetime
from velox_common import *

# args is argparse.Namespace. returns a Cluster().
def get_cluster(args):
    return Cluster(args.region, args.cluster_id, args.num_servers, args.num_clients)

# sub-commands. args is argparse.Namespace.
def command_launch(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    pprint("Launching velox clusters")
    check_for_instances(cluster)

    if args.no_spot:
        provision_instances(cluster, **kwargs)
    else:
        provision_spot(cluster, **kwargs)

    wait_all_hosts_up(cluster)
    command_claim(args)

def command_claim(args):
    cluster = get_cluster(args)
    pprint("Claiming untagged instances...")
    claim_instances(cluster)

def command_rebuild(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    pprint("Rebuilding velox clusters")
    assign_hosts(cluster)
    stop_velox_processes()
    rebuild_servers(**kwargs)

def command_terminate(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    terminate_cluster(cluster, **kwargs)

def command_install_ykit(args):
    cluster = get_cluster(args)
    pprint("Installing Yourkit")
    assign_hosts(cluster)
    install_ykit(cluster)

def command_client_bench(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    runid = "THECRANK-%s" % (str(datetime.now()).replace(' ', '_').replace(":", '_'))
    pprint("Running THE CRANKSHAW")
    assign_hosts(cluster)
    start_servers(cluster, **kwargs)
    sleep(5)
    run_velox_client_bench(cluster, **kwargs)
    stop_velox_processes()
    fetch_logs(cluster, runid, **kwargs)
    pprint("THE CRANKSHAW has completed!")

def command_client_bench_local(args):
    kwargs = dict(vars(args))
    pprint("Running THE CRANKSHAW locally! (1 client only)")
    start_servers_local(**kwargs)
    sleep(5)
    client_bench_local_single(**kwargs)
    kill_velox_local()
    pprint("THE CRANKSHAW has completed!")

def command_ycsb_bench(args):
    cluster = get_cluster(args)
    kwargs = dict(vars(args))
    runid = "YCSB-%s" % (str(datetime.now()).replace(' ', '_').replace(":", '_'))
    pprint("Running YCSB")
    assign_hosts(cluster)
    start_servers(cluster, **kwargs)
    sleep(5)
    run_ycsb(cluster, **kwargs)
    stop_velox_processes()
    fetch_logs(cluster, runid, **kwargs)
    pprint("YCSB has completed!")

def command_ycsb_bench_local(args):
    kwargs = dict(vars(args))
    pprint("Running YCSB locally! (1 client only)")
    start_servers_local(**kwargs)
    sleep(5)
    run_ycsb_local(**kwargs)
    kill_velox_local()
    pprint("YCSB has completed!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup velox on EC2')
    ##################################
    ######### global options #########
    ##################################
    parser.add_argument('--cluster_id', '-c', dest='cluster_id', required=True,
                        help='Cluster ID (tag) to use for your instances')
    parser.add_argument('--num_servers', '-ns', dest='num_servers', type=int, required=True,
                        help='Number of server machines per cluster.')
    parser.add_argument('--num_clients', '-nc', dest='num_clients', type=int, required=True,
                        help='Number of client machines per cluster.')
    subparsers = parser.add_subparsers(title='Sub-Commands',
                                       description='Valid Sub-Commands',
                                       help='Sub-Command Help',
                                       dest='subcommand')

    ##################################
    ######### common options #########
    ##################################
    # common cluster config options for ec2
    common_cluster_ec2 = argparse.ArgumentParser(add_help=False)
    common_cluster_ec2.add_argument('--region', '-r', dest='region', default="us-west-2", type=str,
                                    help="EC2 region. [default: %(default)s]")
    # common benchmark options (base)
    common_benchmark = argparse.ArgumentParser(add_help=False)
    common_benchmark.add_argument('--profile', action='store_true',
                                  help='Run JVM with hprof cpu profiling. [default: %(default)s]')
    common_benchmark.add_argument('--profile_depth', dest='profile_depth', default=2, type=int,
                                  help='Stack depth to trace when running profiling. [default: %(default)s]')
    common_benchmark.add_argument('--usefutures', action='store_true',
                                  help='Have THE CRANKSHAW use futures instead of blocking for reply. [default: %(default)s]')
    common_benchmark.add_argument('--network_service', dest='network_service',
                                  default='array', type=str, choices=['array', 'nio'],
                                  help="Which network service to use. [default: %(default)s]")
    common_benchmark.add_argument('--buffer_size', dest='buffer_size',
                                  default=16384*8, type=int,
                                  help='Size (in bytes) to make the network buffer. [default: %(default)s]')
    common_benchmark.add_argument('--sweep_time', dest='sweep_time',
                                  default=500, type=int,
                                  help='Time (in ms) the ArrayNetworkService send sweep thread should wait between sweeps. [default: %(default)s]')
    # common benchmark options for ec2 (includes benchmark base)
    common_benchmark_ec2 = argparse.ArgumentParser(add_help=False, parents=[common_benchmark])
    common_benchmark_ec2.add_argument('--output', dest='output_dir', default="./output", type=str,
                                      help='output directory for runs. [default: %(default)s]')
    # common crankshaw options
    common_client_bench = argparse.ArgumentParser(add_help=False)
    common_client_bench.add_argument('--latency', action='store_true',
                                     help='Compute average latency when running THE CRANKSHAW. [default: %(default)s]')

    ##################################
    ###### sub-command options #######
    ##################################
    # launch
    parser_launch = subparsers.add_parser('launch', help='Launch EC2 cluster',
                                          parents=[common_cluster_ec2])
    parser_launch.set_defaults(func=command_launch)
    parser_launch.add_argument('--no_spot', dest='no_spot',  action='store_true',
                               help='Don\'t use spot instances. [default: %(default)s]')
    parser_launch.add_argument('--spot_price', dest="spot_price", type=float, default=1.5,
                               help="Spot price in $. [default: %(default)s]")
    parser_launch.add_argument('--instance_type', dest="instance_type", type=str, default="cr1.8xlarge",
                               help="EC2 instance type. [default: %(default)s]")
    parser_launch.add_argument('--placement_group', dest='placement_group', default="VELOX_CLUSTER",
                               help="EC2 placement group. [default: %(default)s]")

    # claim
    parser_claim = subparsers.add_parser('claim', help='Claim non-tagged instances as our own',
                                         parents=[common_cluster_ec2])
    parser_claim.set_defaults(func=command_claim)

    # terminate
    parser_terminate = subparsers.add_parser('terminate', help='Terminate the EC2 cluster and any matching instances',
                                             parents=[common_cluster_ec2])
    parser_terminate.set_defaults(func=command_terminate)

    # rebuild
    parser_rebuild = subparsers.add_parser('rebuild', help='Rebuild velox cluster',
                                           parents=[common_cluster_ec2])
    parser_rebuild.set_defaults(func=command_rebuild)
    parser_rebuild.add_argument('--branch', '-b', dest="branch", default="master",
                        help='Branch to rebuild. [default: %(default)s]')
    parser_rebuild.add_argument('--git_remote', dest="git_remote", default="git@github.com:amplab/velox.git",
                        help='Upstream git url. [default: %(default)s]')
    parser_rebuild.add_argument('--deploy_key', dest="deploy_key", default=None,
                        help='Local path to upstream deploy key. [default: %(default)s]')

    # install_ykit
    parser_install_ykit = subparsers.add_parser('install_ykit', help='Install yourkit',
                                                parents=[common_cluster_ec2])
    parser_install_ykit.set_defaults(func=command_install_ykit)

    ##################################
    ####### benchmark commands #######
    ##################################
    parser_client_bench = subparsers.add_parser('client_bench', help='Run THE CRANKSHAW TEST on EC2',
                                                parents=[common_cluster_ec2, common_benchmark_ec2, common_client_bench])
    parser_client_bench.set_defaults(func=command_client_bench)

    parser_client_bench_local = subparsers.add_parser('client_bench_local', help='Run THE CRANKSHAW TEST locally',
                                                      parents=[common_benchmark, common_client_bench])
    parser_client_bench_local.set_defaults(func=command_client_bench_local)

    parser_ycsb_bench = subparsers.add_parser('ycsb_bench', help='Run YCSB on EC2',
                                              parents=[common_cluster_ec2, common_benchmark_ec2])
    parser_ycsb_bench.set_defaults(func=command_ycsb_bench)

    parser_ycsb_bench_local = subparsers.add_parser('ycsb_bench_local', help='Run YCSB locally',
                                                    parents=[common_benchmark])
    parser_ycsb_bench_local.set_defaults(func=command_ycsb_bench_local)

    # parse the args, and execute the sub-command
    args = parser.parse_args()
    args.func(args)
