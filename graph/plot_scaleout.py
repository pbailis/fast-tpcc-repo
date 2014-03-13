
from os import listdir
from pylab import *

lw=1
padInches=0.0

matplotlib.rcParams['figure.figsize'] = 6, 3
matplotlib.rcParams['lines.linewidth'] = lw
matplotlib.rcParams['axes.linewidth'] = lw   
matplotlib.rcParams['lines.markeredgewidth'] = lw
matplotlib.rcParams['lines.markersize'] = 8
matplotlib.rcParams['font.size'] = 10
matplotlib.rcParams['font.weight'] = 'normal'


conf_results = {}

conf_fmt={'ca': 'o-', 
             'serializable': 's--'}

conf_colors={'ca': 'blue',
             'serializable': 'red'}

conf_labels={'ca': 'CA',
             'serializable': 'S'}

def avg(l):
    return float(sum(l))/len(l)

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()


basedir = "output"
for d in listdir("output"):
    if(d.find('scaleout') == -1):
       continue

    ds= d.split('-')
    conf = ds[3]

    if conf=="serializable":
        continue

    clients = int(ds[1].split("S")[1])
    it = ds[4].split("IT")[1]

    bd1 = basedir+"/"+d+"/"+d
    



    if conf not in conf_results:
        conf_results[conf] = {}

    if clients not in conf_results[conf]:
        conf_results[conf][clients] = []

    thru = 0
    nc = 0

    for sd in listdir(bd1):
        sd_cur = bd1+'/'+sd
        if sd_cur.find("Cec2") == -1:
            continue
        
        f = open(sd_cur+'/client.log')
        found = False
        for line in f:
            if line.find("TOTAL") != -1:
                thru += float(line.split(' ')[2])
                nc += 1
                found = True

        if not found:
            print sd_cur

    print conf, clients, it, thru, nc

    conf_results[conf][clients].append(thru)

for conf in conf_results:
    print conf
    items = conf_results[conf].keys()
    items.sort()

    ax2.plot(items, [avg(conf_results[conf][item]) for item in items], 'o-',  color="blue",  markeredgecolor="blue", markerfacecolor='None')
    ax1.plot(items, [avg(conf_results[conf][item])/item for item in items], 'x-.',  color="green",  markeredgecolor="green", markerfacecolor='None')

    tot_thrus =  [2, 4, 6, 8, 10, 12, 14]
    ax2.set_yticks([i*1000000 for i in tot_thrus])
    ax2.set_yticklabels([str(i)+"M" for i in tot_thrus])
    ax2.set_ylabel("Total Throughput (txn/s)")

    ax1.spines['right'].set_color('blue')
    ax1.spines['left'].set_color('green')


    ax1.set_ylabel("Per-Server Throughput (txn/s)")
    per_thrus =  [64, 68, 72, 76, 80]
    ax1.set_yticks([i*1000 for i in per_thrus])
    ax1.set_yticklabels([str(i)+"K" for i in per_thrus])


    ax1.set_xlabel("Number of Servers")    
    ax1.set_xticks([10, 50, 100, 150, 200])

    for item in items:
        print conf, item, avg(conf_results[conf][item]), avg(conf_results[conf][item])/item

xlim(xmin=5)
#subplots_adjust(bottom=0.2, right=0.85, top=0.9, left=0.14)

savefig("scale_thru.pdf", transparent=True, pad_inches=padInches, bbox_inches='tight')



