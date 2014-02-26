# Common helper functions

from os import system
import subprocess
from time import sleep
from boto import ec2

KEY_NAME = "velox"
VELOX_INTERNAL_PORT_START = 8080
VELOX_FRONTEND_PORT_START = 9000

VELOX_BASE_DIR="/home/ubuntu/velox"

HEAP_SIZE_GB = 240
VELOX_JAR_LOCATION = "assembly/target/scala-2.10/velox-assembly-0.1.jar"
VELOX_SECURITY_GROUP = "velox"

DEFAULT_INSTANCE_TYPE = "cr1.8xlarge"

VELOX_SERVER_CLASS = "edu.berkeley.velox.server.VeloxServer"
VELOX_CLIENT_BENCH_CLASS = "edu.berkeley.velox.benchmark.ClientBenchmark"

AMIs = {'us-west-2': 'ami-8885e5b8',
        'us-east-1': 'ami-b7dbe3de'}

def run_cmd(hosts, cmd, user="ubuntu", time=1000):
    cmd = "pssh -i -t %d -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (time, user, hosts, cmd)
    print cmd
    # print "You may need to install pssh (sudo pip install pssh)"
    system(cmd)

def run_cmd_single(host, cmd, user="ubuntu", time = None):
    cmd = "ssh -o StrictHostKeyChecking=no %s@%s \"%s\"" % (user, host, cmd)
    print cmd
    system(cmd)

def run_cmd_single_bg(host, cmd, user="ubuntu", time = None):
    cmd = "ssh -o StrictHostKeyChecking=no %s@%s \"%s\" &" % (user, host, cmd)
    print cmd
    system(cmd)


def start_cmd_disown(host, cmd, user="ubuntu"):
    run_cmd_single_bg(host, cmd+" & disown", user)


def start_cmd_disown_nobg(host, cmd, user="ubuntu"):
    run_cmd_single_bg(host, cmd+" disown", user)

def run_process_single(host, cmd, user="ubuntu", stdout=None, stderr=None):
    subprocess.call("ssh %s@%s \"%s\"" % (user, host, cmd),
                    stdout=stdout, stderr=stderr, shell=True)

def upload_file(hosts, local_path, remote_path, user="ubuntu"):
    system("cp %s /tmp" % (local_path))
    script = local_path.split("/")[-1]
    system("pscp -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt /tmp/%s %s" % (user, hosts, script, remote_path))

def run_script(hosts, script, user="ubuntu"):
    upload_file(hosts, script.split(" ")[0], "/tmp", user)
    run_cmd(hosts, "bash /tmp/%s" % (script.split("/")[-1]), user)

def fetch_file_single(host, remote, local, user="ubuntu"):
    system("scp -o StrictHostKeyChecking=no %s@%s:%s %s" % (user, host, remote, local))

def fetch_file_single_compressed(host, remote, local, user="ubuntu"):
    print("scp -C -o StrictHostKeyChecking=no %s@%s:%s '%s'" % (user, host, remote, local))

    system("scp -C -o StrictHostKeyChecking=no %s@%s:%s '%s'" % (user, host, remote, local))

def get_host_ips(hosts):
    return open("hosts/%s.txt" % (hosts)).read().split('\n')[:-1]

def sed(file, find, repl):
    iOpt = ''
    print 'sed -i -e %s \'s/%s/%s/g\' %s' % (iOpt, escape(find), escape(repl), file)
    system('sed -i -e %s \'s/%s/%s/g\' %s' % (iOpt, escape(find), escape(repl), file))

def escape(path):
    return path.replace('/', '\/')

def get_node_ips():
    ret = []
    system("ec2-describe-instances > /tmp/instances.txt")
    system("ec2-describe-instances --region us-west-2 >> /tmp/instances.txt")
    for line in open("/tmp/instances.txt"):
        line = line.split()
        if line[0] != "INSTANCE" or line[5] != "running":
            continue
        # addr, externalip, internalip, ami
        ret.append((line[3], line[13], line[14], line[1]))
    return ret

def get_matching_ip(host):
    for h in get_node_ips():
        if h[0] == host:
            return h[1]

def pprint(str):
    print '\033[94m%s\033[0m' % str



## EC2 stuff

def run_cmd_in_velox(hosts, cmd, user='ubuntu'):
    run_cmd(hosts, "cd %s; %s" % (VELOX_BASE_DIR, cmd), user)

class Cluster:
    def __init__(self, regionName, clusterID, numServers, numClients, serversPerMachine):
        self.regionName = regionName
        self.clusterID = clusterID
        self.numServers = numServers*serversPerMachine
        self.servers = []
        self.numClients = numClients
        self.clients = []
        self.serversPerMachine = serversPerMachine

    def allocateHosts(self, hosts):
        for host in hosts:
            if len(self.servers) < self.numServers:
                for i in range(0, self.serversPerMachine):
                    self.servers.append(host)
            elif len(self.clients) < self.numClients:
                self.clients.append(host)

        assert len(self.getAllHosts()) == self.getNumHosts(), "Don't have exactly as many hosts as I expect!" \
                                                          " (expect: %d, have: %d)" \
                                                          % (self.getNumHosts(), len(self.getAllHosts()))

    def getAllHosts(self):
        return self.servers + self.clients

    def getNumHosts(self):
        return self.numServers + self.numClients

class Host:
    def __init__(self, ip, regionName, cluster_id, instanceid, status):
        self.ip = ip
        self.regionName = regionName
        self.cluster_id = cluster_id
        self.instanceid = instanceid
        self.status = status


# Passing cluster_id=None will return all hosts without a tag.
def get_instances(regionName, cluster_id):
    system("rm -f instances.txt")
    hosts = []

    conn = ec2.connect_to_region(regionName)

    filters={'instance-state-name':'running'}
    if cluster_id is not None:
        filters['tag:cluster'] = cluster_id
    reservations = conn.get_all_instances(filters=filters)

    instances = []
    for reservation in reservations:
        instances += reservation.instances

    for i in instances:
        if cluster_id is None and len(i.tags) != 0:
            continue
        hosts.append(Host(str(i.public_dns_name), regionName, cluster_id, str(i.id), str(i.state)))

    return hosts

def get_spot_request_ids(regionName):
    system("rm -f instances.txt")
    global AMIs

    conn = ec2.connect_to_region(regionName)
    return [str(i.id) for i in conn.get_all_spot_instance_requests()]

def get_num_running_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status == "running"])

def get_num_nonterminated_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status != "terminated"])

def make_instancefile(name, hosts):
    f = open("hosts/" + name, 'w')
    for host in hosts:
        f.write("%s\n" % (host.ip))
    f.close

def check_for_instances(region, tag):
    numRunningAnywhere = 0
    numUntagged = 0
    numRunning = get_num_nonterminated_instances(region, tag)
    numRunningAnywhere += numRunning
    numUntagged += get_num_nonterminated_instances(region, None)

    if numRunningAnywhere > 0:
        pprint("NOTICE: You appear to have %d instances up already." % numRunningAnywhere)
        f = raw_input("Continue without terminating them? ")
        if f != "Y" and f != "y":
            exit(-1)

    if numUntagged > 0:
        pprint("NOTICE: You appear to have %d UNTAGGED instances up already." % numUntagged)
        f = raw_input("Continue without terminating/claiming them? ")
        if f != "Y" and f != "y":
            exit(-1)



def terminate_cluster(region, tag, placement_group="velox"):
    allHosts = get_instances(region, tag) + get_instances(region, None)
    instance_ids = [h.instanceid for h in allHosts]
    spot_request_ids = get_spot_request_ids(region)

    conn = ec2.connect_to_region(region)

    if len(instance_ids) > 0:
        pprint('Terminating instances (tagged & untagged) in %s...' % region)
        conn.terminate_instances(instance_ids)
    else:
        pprint('No instances to terminate in %s, skipping...' % region)

    if len(spot_request_ids) > 0:
        pprint('Cancelling spot requests in %s...' % region)
        conn.cancel_spot_instance_requests(spot_request_ids)
    else:
        pprint('No spot requests to cancel in %s, skipping...' % region)

    #conn.delete_placement_group(placement_group)


def provision_spot(regionName, num, instance_type=DEFAULT_INSTANCE_TYPE, bid_price=1.5, placement_group="velox"):
    global AMIs

    setup_security_group(regionName)

    f = raw_input("spinning up %d spot instances in %s; okay? (y/N) " %
                  (num, regionName))

    if f != "Y" and f != "y":
        exit(-1)

    conn = ec2.connect_to_region(regionName)
    '''
    try:
        conn.create_placement_group(placement_group)
    except:
        print "Placement group exception "+placement_group
    '''
    reservations = conn.request_spot_instances(bid_price,
                                               AMIs[regionName],
                                               count=num,
                                               instance_type=instance_type,
                                               security_groups=[VELOX_SECURITY_GROUP])
    #                                           placement_group=placement_group)

def provision_instances(regionName, num, instance_type=DEFAULT_INSTANCE_TYPE, placement_group="velox"):
    global AMIs

    setup_security_group(regionName)

    f = raw_input("spinning up %d instances in %s; okay? (y/N) " %
                  (num, regionName))

    if f != "Y" and f != "y":
        exit(-1)

    conn = ec2.connect_to_region(regionName)
    '''
    try:
        conn.create_placement_group(placement_group)
    except:
        print "Placement group exception "+placement_group
    '''
    reservations = conn.run_instances(AMIs[regionName],
                                       count=num,
                                       instance_type=instance_type,
                                       security_groups=[VELOX_SECURITY_GROUP])
    #                                   placement_group=placement_group)

def wait_all_hosts_up(region, num_hosts):
    pprint("Waiting for instances in %s to start..." % region)
    while True:
        numInstancesInRegion = get_num_running_instances(region, None)
        if numInstancesInRegion >= num_hosts:
            break
        else:
            pprint("Got %d of %d hosts; sleeping..." % (numInstancesInRegion, num_hosts))
        sleep(5)
    pprint("All instances in %s alive!" % region)

    # Since ssh takes some time to come up
    pprint("Waiting for instances to warm up... ")
    sleep(10)
    pprint("Awake!")

def claim_instances(region, cluster_id):
    instances = get_instances(region, None)
    instanceString = ' '.join([host.instanceid for host in instances])
    pprint("Claiming %s..." % instanceString)
    conn = ec2.connect_to_region(region)
    instances = [i.instanceid for i in instances]
    reservations = conn.create_tags(instances, {"cluster":cluster_id})
    pprint("Claimed!")

def setup_security_group(region, group_name=VELOX_SECURITY_GROUP):
    conn = ec2.connect_to_region(region)
    try :
        if len(filter(lambda x: x.name == group_name, conn.get_all_security_groups())) != 0:
            conn.delete_security_group(name=group_name)
        group = conn.create_security_group(group_name, "Velox EC2 all-open SG")
        group.authorize('tcp', 0, 65535, '0.0.0.0/0')
    except Exception as e:
        pprint("Oops; couldn't create a new security group (%s). This is probably fine: "+e % (group_name))


# Assigns hosts to clusters (and specifically as servers, clients)
# Also logs the assignments in the hosts/ files.
def assign_hosts(region, cluster):
    system("mkdir -p hosts")

    hosts = get_instances(region, cluster.clusterID)
    pprint("Assigning %d hosts to %s:% s... " % (len(hosts), region, cluster.clusterID))

    cluster.allocateHosts(hosts[:cluster.getNumHosts()])
    frontend_servers = []
    internal_servers = []
    sid = 0
    for server in cluster.servers:
        frontend_servers.append("%s:%d" % (server.ip, VELOX_FRONTEND_PORT_START+sid))
        internal_servers.append("%s:%d" % (server.ip, VELOX_INTERNAL_PORT_START+sid))
        sid += 1
    cluster.internal_cluster_str = ",".join(internal_servers)
    cluster.frontend_cluster_str = ",".join(frontend_servers)

    # Finally write the instance files for the regions and everything.
    make_instancefile("all-hosts.txt", cluster.getAllHosts())
    make_instancefile("all-servers.txt", cluster.servers)
    make_instancefile("all-clients.txt", cluster.clients)

    pprint("Assigned all %d hosts!" % len(hosts))

def stop_velox_processes():
    pprint("Terminating java processes...")
    run_cmd("all-hosts", "killall java; pkill java")
    sleep(5)
    pprint('Termination command sent.')

def install_ykit(cluster):
    run_cmd("all-hosts", "rm yjp*; rm -rf yourkit*; wget http://www.yourkit.com/download/yjp-2013-build-13072-linux.tar.bz2")
    run_cmd("all-hosts", "tar -xvf yjp-2013-build-13072-linux.tar.bz2")
    run_cmd("all-hosts", "mv yjp-2013-build-13072 yourkit")
    # master = cluster.clients[0].ip
    # run_cmd_single(master, "wget http://www.yourkit.com/download/yjp-2013-build-13072-linux.tar.bz2")
    # run_cmd("all-hosts", "scp ubuntu@%s:yjp-2013-build-13072-linux.tar.bz2 yjp-2013.tar.bz2" %  master)
    # run_cmd("all-hosts", "tar -xvf yjp-2013.tar.bz2")

def rebuild_servers(remote, branch, deploy_key=None):
    if deploy_key:
        upload_file("all-hosts", deploy_key, "/home/ubuntu/.ssh")
        run_cmd("all-hosts", "echo 'IdentityFile /home/ubuntu/.ssh/%s' >> /home/ubuntu/.ssh/config; chmod go-r /home/ubuntu/.ssh/*" % (deploy_key.split("/")[-1]))

    pprint('Rebuilding clients and servers...')
    run_cmd_in_velox('all-hosts',
                     ("rm *.log; git remote rm vremote; "
                      "git remote add vremote %s; "
                      "git checkout master; "
                      "git branch -D veloxbranch; "
                      "git fetch vremote; "
                      "git checkout -b veloxbranch vremote/%s; "
                      "git reset --hard vremote/%s; "
                      "sbt/sbt assembly; ") % (remote, branch, branch))
    pprint('Rebuilt to %s/%s!' % (remote, branch))

def start_servers(cluster, network_service, buffer_size, sweep_time, storage_size, storage_parallelism, profile=False, profile_depth=2, serializable = False, **kwargs):
    HEADER = "pkill -9 java; cd /home/ubuntu/velox/; sleep 2; rm *.log;"

    pstr = ""
    if profile:
        # pstr += "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.server.txt" % (profile_depth)
        pstr += "-agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so"

    baseCmd = HEADER+"java %s -XX:+UseParallelGC -Xms%dG -Xmx%dG -cp %s %s -p %d -f %d --id %d -c %s --network_service %s --buffer_size %d --sweep_time %d --storage_size %d --storage_parallelism %d %s 1>server.log-%d 2>&1 & "

    for sid in range(0, cluster.numServers):
        serverCmd = baseCmd % (
                        pstr,
                        HEAP_SIZE_GB,
                        HEAP_SIZE_GB,
                        VELOX_JAR_LOCATION,
                        VELOX_SERVER_CLASS,
                        VELOX_INTERNAL_PORT_START+sid,
                        VELOX_FRONTEND_PORT_START+sid,
                        sid,
                        cluster.internal_cluster_str,
                        network_service,
                        buffer_size,
                        sweep_time,
                        storage_size,
                        storage_parallelism,
                        "--serializable true" if serializable else "",
                        sid)

        server = cluster.servers[sid]
        pprint("Starting velox server on [%s]" % server.ip)
        start_cmd_disown_nobg(server.ip, serverCmd)

def kill_velox_local():
    system("ps ax | grep Velox | grep java |  sed \"s/[ ]*//\" | cut -d ' ' -f 1 | xargs kill")

def start_servers_local(numservers, network_service, buffer_size, sweep_time, profile=False, depth=2):
    kill_velox_local()

    serverConfigStr = ",".join(["localhost:"+str(VELOX_INTERNAL_PORT_START+id) for id in range(0, numservers)])

    baseCmd = "java %s -XX:+UseParallelGC -Xms128m -Xmx512m -cp %s %s -p %d -f %d --id %d -c %s --network_service %s --buffer_size %d --sweep_time %d 1> /tmp/server-%d.log 2>&1 &"


    for sid in range(0, numservers):
        if profile:
            pstr = "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.server-%d.txt" % (depth,sid)
        else:
            pstr = ""
        serverCmd = baseCmd % (
         pstr,
         VELOX_JAR_LOCATION,
         VELOX_SERVER_CLASS,
         VELOX_INTERNAL_PORT_START+sid,
         VELOX_FRONTEND_PORT_START+sid,
         sid,
         serverConfigStr,
         network_service,
         buffer_size,
         sweep_time,
         sid)
        system(serverCmd)

    pprint("Started servers! Logs in /tmp/server-*.log")

def client_bench_local_single(numservers, network_service, buffer_size, sweep_time, profile, depth, parallelism=64, pct_reads=.5, ops=100000, timeout=3000, futures=True, latency=False):
    clientConfigStr = ",".join(["localhost:"+str(VELOX_FRONTEND_PORT_START+id) for id in range(0, numservers)])
    if profile:
        pstr = "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.client.txt" % depth
    else:
        pstr = ""
    system("java %s -XX:+UseParallelGC -Xms512m -Xmx2G -cp %s %s -m %s --parallelism %d --pct_reads %f --ops %d --timeout %d --network_service %s --buffer_size %d --sweep_time %d" % (
        pstr, VELOX_JAR_LOCATION, VELOX_CLIENT_BENCH_CLASS, clientConfigStr, parallelism, pct_reads, ops, timeout, network_service, buffer_size, sweep_time))


#  -agentlib:hprof=cpu=samples,interval=20,depth=3,monitor=y
def run_velox_client_bench(cluster, network_service, buffer_size, sweep_time, profile=False, profile_depth=2, parallelism=64, chance_remote=.01, ops=100000, timeout=5, connection_parallelism=1, serializable = False):
    hprof = ""

    if profile:
        hprof += "-agentpath:/home/ubuntu/yourkit/bin/linux-x86-64/libyjpagent.so"
        #hprof = "-agentlib:hprof=cpu=samples,interval=20,depth=%d,file=java.hprof.client.txt" % (profile_depth)

    cmd = ("pkill -9 java; "
           "java %s -XX:+UseParallelGC -Xms%dG -Xmx%dG -cp %s %s -m %s --parallelism %d --chance_remote %f --ops %d --timeout %d --network_service %s --buffer_size %d --sweep_time %d --connection_parallelism %d %s --run 2>&1 | tee client.log") %\
          (hprof, HEAP_SIZE_GB, HEAP_SIZE_GB, VELOX_JAR_LOCATION, VELOX_CLIENT_BENCH_CLASS, cluster.frontend_cluster_str,
           parallelism, chance_remote, ops, timeout, network_service, buffer_size, sweep_time, connection_parallelism, "--serializable" if serializable else "")

    load_cmd = cmd.replace("--run", "--load").replace("--serializable", "")
    run_cmd_single(cluster.clients[0].ip, "cd velox; "+load_cmd)

    run_cmd_in_velox("all-clients", cmd)

def run_ycsb_local(numservers, workload="workloads/workloada", threads=64, readprop=.5, valuesize=1, recordcount=10000, request_distribution="zipfian", time=60, dorebuild=True):
    clientConfigStr = ",".join(["localhost:"+str(VELOX_FRONTEND_PORT_START+id) for id in range(0, numservers)])

    ycsb_cmd = (("cd external/ycsb; "
                 "bin/ycsb run velox "
                 "-s "
                 "-P %s "
                 "-threads %d "
                 "-p readproportion=%s "
                 "-p updateproportion=%s "
                 "-p fieldlength=%d "
                 "-p fieldcount=1 "
                 "-p recordcount=%d "
                 "-p operationcount=1000000 "
                 "-p requestdistribution=%s "
                 "-p maxexecutiontime=%d "
                 "-p cluster=%s") %
                (workload, threads, readprop, 1-readprop, valuesize, recordcount, request_distribution, time, clientConfigStr))

    if dorebuild:
        pprint("Rebuilding YCSB")
        system("cd external/ycsb; ./package-ycsb.sh")
        pprint("YCSB rebuilt!")

    pprint("Loading YCSB on single client...")
    load_cmd = ycsb_cmd.replace(" run ", " load ")
    print("cd external/ycsb; "+load_cmd)
    system(load_cmd)
    pprint("YCSB loaded!")

    pprint("Running YCSB")
    print("cd external/ycsb; "+ycsb_cmd)
    system(ycsb_cmd)
    pprint("YCSB complete!")

def run_ycsb(cluster, workload="workloads/workloada", threads=64, readprop=.5, valuesize=1, recordcount=10000, request_distribution="zipfian", time=60, dorebuild=True):
    ycsb_cmd = (("pkill -9 java;"
                 "cd /home/ubuntu/velox/external/ycsb; "
                 "bin/ycsb run velox "
                 "-s "
                 "-P %s "
                 "-threads %d "
                 "-p readproportion=%s "
                 "-p updateproportion=%s "
                 "-p fieldlength=%d "
                 "-p fieldcount=1 "
                 "-p recordcount=%d "
                 "-p operationcount=1000000 "
                 "-p requestdistribution=%s "
                 "-p maxexecutiontime=%d "
                 "-p cluster=%s > run_out.log 2> run_err.log") %
                (workload, threads, readprop, 1-readprop, valuesize, recordcount, request_distribution, time, cluster.frontend_cluster_str))

    if dorebuild:
        pprint("Rebuilding YCSB")
        run_cmd("all-clients", "cd /home/ubuntu/velox/external/ycsb; ./package-ycsb.sh")
        pprint("YCSB rebuilt!")

    pprint("Loading YCSB on single client...")
    load_cmd = ycsb_cmd.replace(" run ", " load ").replace("run_", "load_")
    run_cmd_single(cluster.clients[0].ip, "cd /home/ubuntu/velox/external/ycsb; "+load_cmd)
    pprint("YCSB loaded!")

    pprint("Running YCSB")
    run_cmd_in_velox("all-clients", ycsb_cmd)
    pprint("YCSB complete!")

def mkdir(d):
    system("mkdir -p %s" % d)

def fetch_logs(output_dir, runid, cluster):
    log_dir = "%s/%s/" % (output_dir, runid)
    mkdir(log_dir)
    for server in cluster.servers:
        s_dir = log_dir+"/S"+server.ip
        mkdir(s_dir)
        fetch_file_single_compressed(server.ip, VELOX_BASE_DIR+"/*.log*", s_dir)

    for client in cluster.clients:
        c_dir = log_dir+"/C"+client.ip
        mkdir(c_dir)
        fetch_file_single_compressed(client.ip, VELOX_BASE_DIR+"/*.log", c_dir)
