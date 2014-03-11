import argparse
from time import sleep
from datetime import datetime
from velox_common import *

ITS = range(1, 4)

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
    parser.add_argument('--spot_price', dest="spot_price", type=float, default=3, help="Spot price")
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

    parser.add_argument('--serializable', action='store_true',
                        help='serializable txns')

    parser.add_argument('--client_bench_local', action='store_true',
                        help='Run THE CRANKSHAW TEST locally')

    parser.add_argument('--wh_bench', action='store_true')
    parser.add_argument('--scaleout', action='store_true')

    parser.add_argument('--client_sweep', action='store_true')
    parser.add_argument('--remote_bench', action='store_true')

    parser.add_argument('--usefutures', action='store_true',
                        help='Have THE CRANKSHAW use futures instead of blocking for reply')
    parser.add_argument('--latency', action='store_true',
                        help='Compute average latency when running THE CRANKSHAW')
    parser.add_argument('--network_service', dest='network_service',
                        default='array', type=str,
                        help="Which network service to use [array/nio]")
    parser.add_argument('--buffer_size', dest='buffer_size',
                        default=131072*1.5, type=int,
                        help='Size (in bytes) to make the network buffer')
    parser.add_argument('--sweep_time', dest='sweep_time',
                        default=200, type=int,
                        help='Time the ArrayNetworkService send sweep thread should wait between sweeps')

    parser.add_argument('--servers_per_machine', dest='servers_per_machine',
                        default=1, type=int)
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

    cluster = Cluster(region, cluster_id, num_servers, num_clients, args.servers_per_machine)

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

    if not args.serializable:
        args.profile = True

    if args.wh_bench:
        for it in ITS:
            for wh in [1, 2, 4, 8, 16]:
                for config in ["ca", "serializable"]:
                    runid = "whbench-WH%d-%s-IT%d" % (wh, config, it)
                    assign_hosts(region, cluster)
                    
                    args.output_dir = "output/"+runid
                    
                    extra_args = "--warehouses_per_server %d" % wh
                    if(config == "serializable"):
                        args.serializable = True
                        args.sweep_time = 0
                        clients = 1200
                    else:
                        args.serializable = False
                        clients = 100000
                        args.sweep_time = 200
                        
                    start_servers(cluster, args.network_service, args.buffer_size, args.sweep_time, args.profile, args.profile_depth, serializable=args.serializable)
                    sleep(20)
                    run_velox_client_bench(cluster, args.network_service, args.buffer_size, args.sweep_time,
                                           args.profile, args.profile_depth,
                                           parallelism=16, timeout=120, ops=clients, chance_remote=0.01, connection_parallelism=1, serializable=args.serializable, extra_args=extra_args)
                    stop_velox_processes()
                    fetch_logs(args.output_dir, runid, cluster)
                    pprint("THE CRANKSHAW has completed!")


    if args.scaleout:
        for it in ITS:
            for config in ["ca"]:
                runid = "scaleout-S%d-C%d-%s-IT%d" % (num_servers, num_clients, config, it)
                assign_hosts(region, cluster)

                args.output_dir = "output/"+runid

                if(config == "serializable"):
                    args.serializable = True
                    args.sweep_time = 0
                    clients = 1200
                else:
                    args.serializable = False
                    clients = 100000
                    args.sweep_time = 20

                start_servers(cluster, args.network_service, args.buffer_size, args.sweep_time, args.profile, args.profile_depth, serializable=args.serializable)
                sleep(20)
                run_velox_client_bench(cluster, args.network_service, args.buffer_size, args.sweep_time,
                                       args.profile, args.profile_depth,
                                       parallelism=16, timeout=120, ops=clients, chance_remote=0.01, connection_parallelism=1, serializable=args.serializable)
                stop_velox_processes()
                fetch_logs(args.output_dir, runid, cluster)
                pprint("THE CRANKSHAW has completed!")

    if args.client_sweep:
        for it in ITS:
            for clients in [1, 4, 16, 64, 256, 1024]:#1, 16, 64, 256, 512]:#1, 10, 100, 1000, 10000]:
                for config in ["serializable", "ca"]:
                    runid = "client_sweep-CLIENTS%d-%s-IT%d" % (clients, config, it)
                    assign_hosts(region, cluster)
                    
                    args.output_dir = "output/"+runid
                    
                    extra_args = ""
                    if(config == "serializable"):
                        args.serializable = True
                        args.sweep_time = 0

                    else:
                        args.serializable=False
                        args.sweep_time = 0

                    start_servers(cluster, args.network_service, args.buffer_size, args.sweep_time, args.profile, args.profile_depth, serializable=args.serializable)
                    sleep(15)
                    run_velox_client_bench(cluster, args.network_service, args.buffer_size, args.sweep_time,
                                           args.profile, args.profile_depth,
                                           parallelism=16, timeout=120, ops=clients, chance_remote=0.01, connection_parallelism=1, serializable=args.serializable, extra_args=extra_args)
                    stop_velox_processes()
                    fetch_logs(args.output_dir, runid, cluster)
                    pprint("THE CRANKSHAW has completed!")

    if args.remote_bench:
       for it in ITS:
           for remote in [0, .05, .1, .25, .5, .75, 1]:# .25, .5, .75, 1]:
               for config in ["ca", "serializable"]:
                   runid = "remotebench-PCT%f-%s-IT%d" % (remote, config, it)
                   assign_hosts(region, cluster)

                   args.output_dir = "output/"+runid

                   extra_args = "--pct_test"
                   if(config == "serializable"):
                       args.serializable = True
                       args.sweep_time = 0
                       if remote == 0:
                           clients = 500
                       else:
                           clients = 1000
                       thread_handlers = False
                       outbound_conn_degree =1

                   else:
                        args.serializable = False
                        args.sweep_time = 200
                        clients = 100000
                        args.buffer_size = 131072*3
                        if remote > 0:
                            thread_handlers = False
                            outbound_conn_degree = 1
                        else:
                            thread_handlers = False
                            outbound_conn_degree = 1

                   start_servers(cluster, args.network_service, args.buffer_size, args.sweep_time, args.profile, args.profile_depth, serializable=args.serializable, thread_handlers=thread_handlers, outbound_conn_degree=outbound_conn_degree)
                   sleep(15)
                   run_velox_client_bench(cluster, args.network_service, args.buffer_size, args.sweep_time,
                                          args.profile, args.profile_depth,
                                          parallelism=16, timeout=120, ops=clients, chance_remote=remote, connection_parallelism=1, serializable=args.serializable, extra_args=extra_args)
                   stop_velox_processes()
                   fetch_logs(args.output_dir, runid, cluster)
                   pprint("THE CRANKSHAW has completed!")

    if args.client_bench:
        runid = "THECRANK-%s" % (str(datetime.now()).replace(' ', '_').replace(":", '_'))
        pprint("Running THE CRANKSHAW")
        assign_hosts(region, cluster)
        start_servers(cluster, args.network_service, args.buffer_size, args.sweep_time, args.profile, args.profile_depth, serializable=args.serializable)
        sleep(5)
        run_velox_client_bench(cluster, args.network_service, args.buffer_size, args.sweep_time,
                               args.profile, args.profile_depth,
                               parallelism=16, timeout=120, ops=100000, chance_remote=0.01, connection_parallelism=1, serializable=args.serializable
                               )
        stop_velox_processes()
        fetch_logs(args.output_dir, runid, cluster)
        pprint("THE CRANKSHAW has completed!")

    if args.client_bench_local:
        pprint("Running THE CRANKSHAW locally! (1 client only)")
        start_servers_local(num_servers, args.network_service, args.buffer_size, args.sweep_time, args.profile, args.profile_depth)
        sleep(5)
        client_bench_local_single(num_servers, args.network_service, args.buffer_size, args.sweep_time,
                                  args.profile, args.profile_depth,
                                  parallelism=1, timeout=45, ops=1000000, pct_reads=0.5,
                                  futures=args.usefutures,latency=args.latency)
        kill_velox_local()
        pprint("THE CRANKSHAW has completed!")
