
from os import listdir
from pylab import *

lw=1
padInches=0.05

matplotlib.rcParams['figure.figsize'] = 3.5, 2
matplotlib.rcParams['lines.linewidth'] = lw
matplotlib.rcParams['axes.linewidth'] = lw   
matplotlib.rcParams['lines.markeredgewidth'] = lw
matplotlib.rcParams['lines.markersize'] = 6
matplotlib.rcParams['font.size'] = 8
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

basedir = "output"
for d in listdir("output"):
    if(d.find('remotebench') == -1):
       continue

    ds= d.split('-')
    conf = ds[2]
    clients = float(ds[1].split("PCT")[1])
    it = ds[3].split("IT")[1]

    bd1 = basedir+"/"+d+"/"+d
    



    if conf not in conf_results:
        conf_results[conf] = {}

    if clients not in conf_results[conf]:
        conf_results[conf][clients] = []

    for sd in listdir(bd1):
        sd_cur = bd1+'/'+sd
        if sd_cur.find("Cec2") == -1:
            continue
        
        thru = 0
        f = open(sd_cur+'/client.log')
        
        for line in f:
            if line.find("TOTAL") != -1:
                thru += float(line.split(' ')[2])



        conf_results[conf][clients].append(thru)

for conf in conf_results:
    print conf
    items = conf_results[conf].keys()
    items.sort()


    baseline = avg(conf_results[conf][0])
    plot([i*100 for i in items], [avg(conf_results[conf][item])/baseline*100 for item in items], conf_fmt[conf],  color=conf_colors[conf],  markeredgecolor=conf_colors[conf], markerfacecolor='None', label=conf_labels[conf])

    for item in items:
        print conf, item, avg(conf_results[conf][item])


xlabel("Percentage Distributed Transactions")
ylabel("Relative Throughput (%)")
#yscale('log')
xticks([0, 25, 50, 75, 100])

l = legend(loc="upper right", ncol=3, handlelength=2)
l.draw_frame(False)

subplots_adjust(bottom=0.2, right=0.95, top=0.9, left=0.14)

savefig("remote_thru.pdf", transparent=True, pad_inches=padInches)



