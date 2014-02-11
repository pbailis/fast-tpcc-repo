import argparse
from time import sleep
from datetime import datetime
from velox_common import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup velox on EC2')

    # required configuration options
    parser.add_argument('--cluster_id', '-c', dest='cluster_id', required=True,
                        help='Cluster ID (tag) to use for your instances')

    # common configuration options
    parser.add_argument('--region', '-r', dest='region',
                        default="us-west-2", type=str)
    parser.add_argument('--num_servers', '-ns', dest='num_servers', nargs='?',
                            default=2, type=int,
                            help='Number of server machines per cluster, default=2')
    parser.add_argument('--num_clients', '-nc', dest='num_clients', nargs='?',
                        default=2, type=int,
                        help='Number of client machines per cluster, default=2')

    # launch options
    parser.add_argument('--no_spot', dest='no_spot', action='store_true',
                        help='Don\'t use spot instances, default off.')
    parser.add_argument('--spot_price', dest="spot_price", type=float, default=1.5, help="Spot price")
    parser.add_argument('--instance_type', dest="instance_type", type=str, default="cr1.8xlarge",
                        help="EC2 instance type")

    parser.add_argument('--placement_group', dest='placement_group', default="VELOX_CLUSTER")

    # rebuild options
    parser.add_argument('--branch', '-b', dest="branch", default="master",
                        help='Branch to rebuild')
    parser.add_argument('--git_remote', dest="git_remote", default="git@github.com:amplab/velox.git",
                        help='Upstream git url')
    parser.add_argument('--deploy_key', dest="deploy_key", default=None,
                        help='Upstream deploy key')

    parser.add_argument('--output', dest='output_dir', nargs='?',
                        default="./output", type=str,
                        help='output directory for runs')

    # actual actions we can run
    parser.add_argument('--launch', '-l', action='store_true',
                        help='Launch EC2 cluster')
    parser.add_argument('--claim', action='store_true',
                        help='Claim non-tagged instances as our own')
    parser.add_argument('--terminate', '-t', action='store_true',
                        help='Terminate the EC2 cluster and any matching instances')
    parser.add_argument('--rebuild', '-rb', action='store_true',
                        help='Rebuild velox cluster')
    parser.add_argument('--install_ykit', '-yk', action='store_true',
                        help='Install yourkit')


    parser.add_argument('--client_bench', action='store_true',
                        help='Run THE CRANKSHAW TEST on EC2')
    parser.add_argument('--client_bench_local', action='store_true',
                        help='Run THE CRANKSHAW TEST locally')
    parser.add_argument('--usefutures', action='store_true',
                        help='Have THE CRANKSHAW use futures instead of blocking for reply')
    parser.add_argument('--network_service', dest='network_service',
                        default='array', type=str,
                        help="Which network service to use [array/nio]")

    # jvm options
    parser.add_argument('--profile', action='store_true',
                        help='Run JVM with hprof cpu profiling')
    parser.add_argument('--profile_depth', dest='profile_depth', nargs='?',
                        default=2, type=int,
                        help='Stack depth to trace when running profiling, default=2')

    parser.add_argument('--ycsb_bench', action='store_true',
                        help='Run YCSB on EC2')
    parser.add_argument('--ycsb_bench_local', action='store_true',
                        help='Run YCSB locally')

    args, unknown = parser.parse_known_args()

    for u in unknown:
        pprint("Unknown argument: "+u)

    region = args.region
    cluster_id = args.cluster_id
    num_servers = args.num_servers
    num_clients = args.num_clients

    cluster = Cluster(region, cluster_id, num_servers, num_clients)

    if args.launch:
        pprint("Launching velox clusters")
        check_for_instances(region, cluster_id)

        num_hosts = num_clients+num_servers
        region = args.region

        if args.no_spot:
            provision_instances(region, num_hosts, instance_type=args.instance_type)
        else:
            provision_spot(region, num_hosts, instance_type=args.instance_type, bid_price=args.spot_price)

        wait_all_hosts_up(region, num_hosts)

    if args.launch or args.claim:
        pprint("Claiming untagged instances...")
        claim_instances(region, cluster_id)

    if args.rebuild:
        pprint("Rebuilding velox clusters")
        assign_hosts(region, cluster)
        stop_velox_processes()
        rebuild_servers(branch=args.branch, remote=args.git_remote, deploy_key=args.deploy_key)

    if args.install_ykit:
        pprint("Installing Yourkit")
        assign_hosts(region, cluster)
        install_ykit(cluster)

    if args.terminate:
        terminate_cluster(region, cluster_id)

    if args.client_bench:
        runid = "THECRANK-%s" % (str(datetime.now()).replace(' ', '_').replace(":", '_'))
        pprint("Running THE CRANKSHAW")
        assign_hosts(region, cluster)
        start_servers(cluster, args.network_service, args.profile, args.profile_depth)
        sleep(5)
        run_velox_client_bench(cluster, args.network_service, args.profile, args.profile_depth, parallelism=16, timeout=30, ops=100000000, pct_reads=.5)
        stop_velox_processes()
        fetch_logs(args.output_dir, runid, cluster)
        pprint("THE CRANKSHAW has completed!")

    if args.ycsb_bench:
        runid = "YCSB-%s" % (str(datetime.now()).replace(' ', '_').replace(":", '_'))
        pprint("Running YCSB")
        assign_hosts(region, cluster)
        start_servers(cluster, args.network_service)
        sleep(5)
        run_ycsb(cluster, threads=64, readprop=.5, valuesize=1, recordcount=10000, time=60)
        stop_velox_processes()
        fetch_logs(args.output_dir, runid, cluster)
        pprint("YCSB has completed!")

    if args.client_bench_local:
        pprint("Running THE CRANKSHAW locally! (1 client only)")
        start_servers_local(num_servers, args.network_service, args.profile, args.profile_depth)
        sleep(5)
        client_bench_local_single(num_servers, args.network_service, args.profile, args.profile_depth,
                                  parallelism=64, timeout=45, ops=100000, pct_reads=0.5, futures=args.usefutures)
        kill_velox_local()
        pprint("THE CRANKSHAW has completed!")

    if args.ycsb_bench_local:
        pprint("Running YCSB locally! (1 client only)")
        start_servers_local(num_servers, args.network_service, args.profile, args.profile_depth)
        sleep(5)
        run_ycsb_local(num_servers, threads=64, readprop=.5, valuesize=1, recordcount=10000, time=60)
        kill_velox_local()
        pprint("YCSB has completed!")
