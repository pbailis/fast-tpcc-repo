
from os import listdir
from pylab import *

lw=1
padInches=0.05

matplotlib.rcParams['figure.figsize'] = 3.3, 2
matplotlib.rcParams['lines.linewidth'] = lw
matplotlib.rcParams['axes.linewidth'] = lw   
matplotlib.rcParams['lines.markeredgewidth'] = lw
matplotlib.rcParams['lines.markersize'] = 6
matplotlib.rcParams['font.size'] = 8
matplotlib.rcParams['font.weight'] = 'normal'


conf_results = {}

conf_fmt={'cfree': 'o-', 
             'twopl': 's--',
             'opt_twopl': '^-'}

conf_colors={'cfree': 'blue',
             'twopl': 'green',
             'opt_twopl': 'red'}

conf_labels={'cfree': 'Coordination-Free',
             'twopl': '2PL',
             'opt_twopl': 'Optimized 2PL'}

def avg(l):
    return float(sum(l))/len(l)

for d in listdir('output-micro'):
    ds= d.split('-')
    if len(ds) < 3:
        continue
    conf = ds[0]
    items = int(ds[1].split("ITEMS")[1])
    if items > 7:
        continue
    it = ds[2].split("IT")[1]


    d_cur = 'output-micro/'+d
    
    if conf not in conf_results:
        conf_results[conf] = {}

    if items not in conf_results[conf]:
        conf_results[conf][items] = []

    for sd in listdir(d_cur):
        sd_cur = d_cur+'/'+sd
        thru = 0
        for sdd in listdir(sd_cur):
            if sdd.find("Cec2") == -1:
                continue
            
            f = open(sd_cur+'/'+sdd+'/client.log')

            for line in f:
                if line.find("TOTAL") != -1:
                    thru += float(line.split(' ')[2])

        conf_results[conf][items].append(thru)

for conf in conf_results:
    print conf
    items = conf_results[conf].keys()
    items.sort()

    plot(items, [avg(conf_results[conf][item]) for item in items], conf_fmt[conf],  color=conf_colors[conf],  markeredgecolor=conf_colors[conf], markerfacecolor='None', label=conf_labels[conf])

    for item in items:
        print conf, item, avg(conf_results[conf][item])


yscale('log')
xlabel("Number of Items Written per Transaction")
ylabel("Throughput (txns/s)")
ylim(ymax=500000000)

l = legend(loc="upper right", ncol=2, handlelength=2)
l.draw_frame(False)

subplots_adjust(bottom=0.22, right=0.95, top=0.9, left=0.14)

savefig("micro_thru_all.pdf", transparent=True, pad_inches=padInches)



